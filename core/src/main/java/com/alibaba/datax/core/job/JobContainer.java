package com.alibaba.datax.core.job;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.container.util.HookInvoker;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import com.alibaba.datax.core.job.meta.ExecuteMode;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报 但它并不做实际的数据同步操作 */
public class JobContainer extends AbstractContainer {
  private static final Logger LOG = LoggerFactory.getLogger(JobContainer.class);
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();

  private long jobId;

  /** reader 和 writer 插件名称 */
  private String readerPluginName;
  private String writerPluginName;

  /** reader和writer jobContainer的实例 */
  private Reader.Job jobReader;
  private Writer.Job jobWriter;
  private Configuration userConf;

  /** 开始时间戳与结束时间戳 */
  private long startTimeStamp;
  private long endTimeStamp;

  /** Transfer 开始时间戳与结束时间戳 */
  private long startTransferTimeStamp;
  private long endTransferTimeStamp;

  /** 需要的 channel 数量 */
  private int needChannelNumber = 1;

  /** 策略数量  */
  private int totalStage = 1;

  /** 错误记录占比 */
  private ErrorRecordChecker errorLimit;

  public JobContainer(Configuration configuration) {
    super(configuration);
    errorLimit = new ErrorRecordChecker(configuration);
  }

  /** 启动 Job 容器 */
  @Override
  public void start() {
    LOG.info("DataX jobContainer starts job.");
    boolean hasException = false;
    boolean isDryRun = false;
    try {
      this.startTimeStamp = System.currentTimeMillis();
      isDryRun = configuration.getBool(CoreConstant.DATAX_JOB_SETTING_DRYRUN, false);
      if (isDryRun){
        LOG.info("jobContainer starts to do dryRunCheck ...");
        this.dryRunCheck();
      } else {
        this.userConf = configuration.clone();

        LOG.debug("jobContainer starts to do init ...");
        this.init();
        LOG.info("jobContainer starts to do split ...");
        this.totalStage = this.split();
        LOG.info("jobContainer starts to do schedule ...");
        this.schedule();
        LOG.info("DataX jobId [{}] completed successfully.", this.jobId);
        this.invokeHooks();
      }
    } catch (Throwable e) {
      LOG.error("Exception when job run", e);
      if (e instanceof OutOfMemoryError) {
        this.destroy();
        System.gc();
      }

      hasException = true;
      if (super.getContainerCommunicator() == null) {
        // 由于 containerCollector 是在 scheduler() 中初始化的，所以当在 scheduler() 之前出现异常时，需要在此处对
        // containerCollector 进行初始化

        AbstractContainerCommunicator tempContainerCollector;
        // standalone
        tempContainerCollector = new StandAloneJobContainerCommunicator(configuration);

        super.setContainerCommunicator(tempContainerCollector);
      }

      Communication communication = super.getContainerCommunicator().collect();
      // 汇报前的状态，不需要手动进行设置
      // communication.setState(State.FAILED);
      communication.setThrowable(e);
      communication.setTimestamp(this.endTimeStamp);

      Communication tempComm = new Communication();
      tempComm.setTimestamp(this.startTransferTimeStamp);

      Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
      super.getContainerCommunicator().report(reportCommunication);

      throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
    } finally {
      if (!isDryRun) {
        this.destroy();
        this.endTimeStamp = System.currentTimeMillis();
        if (!hasException) {
          // 最后打印cpu的平均消耗，GC的统计
          VMInfo vmInfo = VMInfo.getVmInfo();
          if (vmInfo != null) {
            vmInfo.getDelta(false);
            LOG.info(vmInfo.totalString());
          }
          LOG.info(PerfTrace.getInstance().summarizeNoException());
          this.logStatistics();
        }
      }
    }
  }

  /** reader和writer的初始化 */
  private void init() {
    this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);
    if (this.jobId < 0) {
      LOG.info("Set jobId = 0");
      this.jobId = 0;
      this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, this.jobId);
    }
    Thread.currentThread().setName("job-" + this.jobId);
    JobPluginCollector jobPluginCollector =
        new DefaultJobPluginCollector(this.getContainerCommunicator());
    // 必须先Reader ，后Writer
    this.jobReader = this.initJobReader(jobPluginCollector);
    this.jobWriter = this.initJobWriter(jobPluginCollector);
  }

    /**
   * reader job的初始化，返回Reader.Job
   *
   * @return
   */
  private Reader.Job initJobReader(JobPluginCollector jobPluginCollector) {
    this.readerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
    classLoaderSwapper.setCurrentThreadClassLoader( LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));

    Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);

    // 设置reader的jobConfig
    jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

    // 设置reader的readerConfig
    jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

    jobReader.setJobPluginCollector(jobPluginCollector);
    jobReader.init();

    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return jobReader;
  }

  /**
   * writer job的初始化，返回Writer.Job
   *
   * @return
   */
  private Writer.Job initJobWriter(JobPluginCollector jobPluginCollector) {
    this.writerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));

    Writer.Job jobWriter =  (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);

    // 设置writer的jobConfig
    jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
    // 设置reader的readerConfig
    jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

    jobWriter.setPeerPluginName(this.readerPluginName);
    jobWriter.setJobPluginCollector(jobPluginCollector);
    jobWriter.init();
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return jobWriter;
  }


  /**
   * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
   * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起， 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
   */
  private int split() {
    this.adjustChannelNumber();
    if (this.needChannelNumber <= 0) {
      this.needChannelNumber = 1;
    }
    List<Configuration> readerTaskConfigs = this.doReaderSplit(this.needChannelNumber);
    int taskNumber = readerTaskConfigs.size();
    List<Configuration> writerTaskConfigs = this.doWriterSplit(taskNumber);
    List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

    LOG.debug("transformer configuration: " + JSON.toJSONString(transformerList));
    /** 输入是reader和writer的parameter list，输出是content下面元素的list */
    List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs, transformerList);
    LOG.debug("contentConfig configuration: " + JSON.toJSONString(contentConfig));
    this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);
    return contentConfig.size();
  }

  /**
   * Reader 任务拆分
   * @param adviceNumber
   * @return
   */
  private List<Configuration> doReaderSplit(int adviceNumber) {
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
    List<Configuration> readerSlicesConfigs = this.jobReader.split(adviceNumber);
    if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
      throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "reader切分的task数目不能小于等于0");
    }
    LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.", this.readerPluginName, readerSlicesConfigs.size());
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return readerSlicesConfigs;
  }

  /**
   * Writer 任务拆分
   * @param readerTaskNumber
   * @return
   */
  private List<Configuration> doWriterSplit(int readerTaskNumber) {
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
    List<Configuration> writerSlicesConfigs = this.jobWriter.split(readerTaskNumber);
    if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
      throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "writer切分的task不能小于等于0");
    }
    LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.", this.writerPluginName, writerSlicesConfigs.size());
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return writerSlicesConfigs;
  }

  /**
   * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
   *
   * @param readerTasksConfigs
   * @param writerTasksConfigs
   * @return
   */
  private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs, List<Configuration> writerTasksConfigs) {
    return mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, null);
  }

  private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs, List<Configuration> writerTasksConfigs, List<Configuration> transformerConfigs) {
//    if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
//      throw DataXException.asDataXException(
//          FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
//          String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",readerTasksConfigs.size(), writerTasksConfigs.size()));
//    }
    List<Configuration> contentConfigs = new ArrayList<Configuration>();
    if (readerTasksConfigs.size() > writerTasksConfigs.size()){
      for (int i = 0; i < readerTasksConfigs.size(); i++) {
        // 获取 Reader 表名，用于查找对应的 Writer
        Configuration jobReaderParameter = readerTasksConfigs.get(i);
        String tableName = jobReaderParameter.getString("table");
        Optional<Configuration> writerTasksConfig = writerTasksConfigs.stream().filter(o -> o.getString("table").equals(tableName)).findFirst();
        if (!writerTasksConfig.isPresent()) continue;

        Configuration taskConfig = Configuration.newDefault();
        taskConfig.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
        taskConfig.set(CoreConstant.JOB_READER_PARAMETER, jobReaderParameter);
        taskConfig.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
        taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER, writerTasksConfig.get());
        if (transformerConfigs != null && transformerConfigs.size() > 0) {
          taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
        }
        taskConfig.set(CoreConstant.TASK_ID, i);
        contentConfigs.add(taskConfig);
      }
    } else {
      for (int i = 0; i < writerTasksConfigs.size(); i++) {
        // 获取 Writer 表名,于查找对应的 Reader
        Configuration jobWriterParameter = writerTasksConfigs.get(i);
        String tableName = jobWriterParameter.getString("table");
        Optional<Configuration> readerTasksConfig = readerTasksConfigs.stream().filter(o -> o.getString("table").equals(tableName)).findFirst();
        if (!readerTasksConfig.isPresent()) continue;

        Configuration taskConfig = Configuration.newDefault();
        taskConfig.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
        taskConfig.set(CoreConstant.JOB_READER_PARAMETER, readerTasksConfig.get());
        taskConfig.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
        taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER, jobWriterParameter);
        if (transformerConfigs != null && transformerConfigs.size() > 0) {
          taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
        }
        taskConfig.set(CoreConstant.TASK_ID, i);
        contentConfigs.add(taskConfig);
      }
    }
    return contentConfigs;
  }


    /**
   //   * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
   //   * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
   //   */
  private void schedule() {
    /** 这里的全局speed和每个channel的速度设置为B/s */
    int channelsPerTaskGroup = this.configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
    int taskNumber = this.configuration.getList(CoreConstant.DATAX_JOB_CONTENT).size();

    this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
    PerfTrace.getInstance().setChannelNumber(needChannelNumber);

    /** 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务 */
    List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration, this.needChannelNumber, channelsPerTaskGroup);
    LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());

    ExecuteMode executeMode = ExecuteMode.STANDALONE;
    AbstractScheduler scheduler;
    try{
      scheduler = initStandaloneScheduler(this.configuration);

      // 设置 executeMode
      for (Configuration taskGroupConfig : taskGroupConfigs) {
        taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, executeMode.getValue());
      }
      LOG.info("Running by {} Mode.", executeMode);
      this.startTransferTimeStamp = System.currentTimeMillis();
      scheduler.schedule(taskGroupConfigs);
      this.endTransferTimeStamp = System.currentTimeMillis();
    } catch (Exception e) {
      LOG.error("运行scheduler 模式[{}]出错.", executeMode);
      this.endTransferTimeStamp = System.currentTimeMillis();
      throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
    }

    /** 检查任务执行情况 */
    this.checkLimit();
  }

    /**
   * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
   *
   * @param
   */
  private void checkLimit() {
    Communication communication = super.getContainerCommunicator().collect();
    errorLimit.checkRecordLimit(communication);
    errorLimit.checkPercentageLimit(communication);
  }

  /**
   * 初始化单节点调度器
   * @param configuration
   * @return
   */
  private AbstractScheduler initStandaloneScheduler(Configuration configuration) {
    AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
    super.setContainerCommunicator(containerCommunicator);
    return new StandAloneScheduler(containerCommunicator);
  }

  /**
   * 计算 Channel 个数
   */
  private void adjustChannelNumber() {
    int needChannelNumberByByte = Integer.MAX_VALUE;
    int needChannelNumberByRecord = Integer.MAX_VALUE;

    // 是否设置了 channel 个数
    boolean isChannelLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
    if (isChannelLimit) {
      this.needChannelNumber = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);
      LOG.info("Job set Channel-Number to " + this.needChannelNumber + " channels.");
      return;
    }

    boolean isByteLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
    if (isByteLimit) {
      long globalLimitedByteSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);
      // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
      Long channelLimitedByteSpeed = this.configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
      if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
        throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
      }
      needChannelNumberByByte = (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
      needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
      LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
    }

    boolean isRecordLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
    if (isRecordLimit) {
      long globalLimitedRecordSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);
      Long channelLimitedRecordSpeed = this.configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
      if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
        throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
      }
      needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
      needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
      LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
    }

    // 取较小值
    this.needChannelNumber = Math.min(needChannelNumberByByte, needChannelNumberByRecord);
    // 如果从byte或record上设置了needChannelNumber则退出
    if (this.needChannelNumber < Integer.MAX_VALUE) {
      return;
    }

    throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "Job运行速度必须设置");
  }

  /**
   * 试运行检查
   */
  private void dryRunCheck(){
    this.dryRunCheckInit();
    this.adjustChannelNumber();
    this.dryRunCheckReader();
    this.dryRunCheckWriter();
    LOG.info("dryRunCheck 通过");
  }
  /**
   * 试运行检查初始化
   */
  private void dryRunCheckInit(){
    this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);
    if (this.jobId < 0) {
      LOG.info("Set jobId = 0");
      this.jobId = 0;
      this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, this.jobId);
    }
    Thread.currentThread().setName("job-" + this.jobId);
    // reader 和 writer 初始化
    JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
    this.jobReader = this.dryRunCheckReaderInit(jobPluginCollector);
    this.jobWriter = this.dryRunCheckWriterInit(jobPluginCollector);
  }

  /**
   * Reader 试运行初始化
   * @param jobPluginCollector
   * @return
   */
  private Reader.Job dryRunCheckReaderInit(JobPluginCollector jobPluginCollector){
    this.readerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
    Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);
    this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

    // 设置reader的jobConfig
    jobReader.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
    // 设置reader的readerConfig
    jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

    jobReader.setJobPluginCollector(jobPluginCollector);
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return jobReader;
  }

  /**
   * Writer 试运行初始化
   * @param jobPluginCollector
   * @return
   */
  private Writer.Job dryRunCheckWriterInit(JobPluginCollector jobPluginCollector) {
    this.writerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
    Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);
    this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

    // 设置writer的jobConfig
    jobWriter.setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
    // 设置reader的readerConfig
    jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

    jobWriter.setPeerPluginName(this.readerPluginName);
    jobWriter.setJobPluginCollector(jobPluginCollector);
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return jobWriter;
  }

  /**
   * Reader 试运行
   */
  private void dryRunCheckReader() {
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
    LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .", this.readerPluginName));
    this.jobReader.preCheck();
    classLoaderSwapper.restoreCurrentThreadClassLoader();
  }

  /**
   * Writer 试运行
   */
  private void dryRunCheckWriter() {
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
    LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .", this.writerPluginName));
    this.jobWriter.preCheck();
    classLoaderSwapper.restoreCurrentThreadClassLoader();
  }


  private void destroy() {
    if (this.jobWriter != null) {
      this.jobWriter.destroy();
      this.jobWriter = null;
    }
    if (this.jobReader != null) {
      this.jobReader.destroy();
      this.jobReader = null;
    }
  }

  private void logStatistics() {
    long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
    long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
    if (0L == transferCosts) {
      transferCosts = 1L;
    }

    if (super.getContainerCommunicator() == null) {
      return;
    }

    Communication communication = super.getContainerCommunicator().collect();
    communication.setTimestamp(this.endTimeStamp);

    Communication tempComm = new Communication();
    tempComm.setTimestamp(this.startTransferTimeStamp);

    Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);

    // 字节速率
    long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / transferCosts;
    long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS) / transferCosts;

    reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
    reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

    super.getContainerCommunicator().report(reportCommunication);

    LOG.info(
        String.format(
            "\n"
                + "%-26s: %-18s\n"
                + "%-26s: %-18s\n"
                + "%-26s: %19s\n"
                + "%-26s: %19s\n"
                + "%-26s: %19s\n"
                + "%-26s: %19s\n"
                + "%-26s: %19s\n",
            "任务启动时刻",
            dateFormat.format(startTimeStamp),
            "任务结束时刻",
            dateFormat.format(endTimeStamp),
            "任务总计耗时",
            String.valueOf(totalCosts) + "s",
            "任务平均流量",
            StrUtil.stringify(byteSpeedPerSecond) + "/s",
            "记录写入速度",
            String.valueOf(recordSpeedPerSecond) + "rec/s",
            "读出记录总数",
            String.valueOf(CommunicationTool.getTotalReadRecords(communication)),
            "读写失败总数",
            String.valueOf(CommunicationTool.getTotalErrorRecords(communication))));

    if (communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
        || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
        || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
      LOG.info(
          String.format(
              "\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n",
              "Transformer成功记录总数",
              communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),
              "Transformer失败记录总数",
              communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),
              "Transformer过滤记录总数",
              communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)));
    }
  }

  /** 调用外部hook */
  private void invokeHooks() {
    Communication comm = super.getContainerCommunicator().collect();
    HookInvoker invoker = new HookInvoker(CoreConstant.DATAX_HOME + "/hook", configuration, comm.getCounter());
    invoker.invokeAll();
  }
}
