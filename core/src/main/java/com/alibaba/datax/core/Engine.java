package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑 */
public class Engine {
  private static final Logger LOG = LoggerFactory.getLogger(Engine.class);
  private static String RUNTIME_MODE;

  /**
   * Engine 入口
   *
   * @param args
   * @throws Throwable
   */
  public static void entry(final String[] args) throws Throwable {

    Options options = new Options();
    options.addOption("job", true, "Job config.");
    options.addOption("jobid", true, "Job unique id.");
    options.addOption("mode", true, "Job runtime mode.");

    // 获取命令行参数
    CommandLine cl = new BasicParser().parse(options, args);
    // 获取 job 配置文件地址
    String jobPath = cl.getOptionValue("job");
    // 获取 jobid，未指定则默认值为 -1
    String jobId = cl.getOptionValue("jobid");
    // datax 运行模式
    RUNTIME_MODE = cl.getOptionValue("mode");

    // 解析 job 配置
    Configuration configuration = ConfigParser.parse(jobPath);
    configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, StringUtils.isNotEmpty(jobId) ? jobId: -1);

    //打印vmInfo
    VMInfo vmInfo = VMInfo.getVmInfo();
    if (vmInfo != null) {
      LOG.info(vmInfo.toString());
    }

    LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");
    LOG.debug(configuration.toJSON());

    Engine.start(configuration);
  }

  /**
   * 启动 job 和 task
   * @param allConf
   */
  private static void start(Configuration allConf) {
    // 绑定column转换信息
    ColumnCast.bind(allConf);
    // 初始化PluginLoader，可以获取各种插件配置
    LoadUtil.bind(allConf);
    allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);

    // 更改后的版本 reader 和 writer 的数据源只能是一对多的关系
    List<Object> readerConnection = allConf.getList(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER_CONNECTION);
    List<Object> writerConnection = allConf.getList(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER_CONNECTION);
    if (readerConnection.size() > 1 && writerConnection.size() > 1){
      throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,"Reader 和 Writer 数据源只能是一对多的关系");
    }

    boolean isJob = !("taskGroup".equalsIgnoreCase(allConf.getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
    long instanceId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);

    //缺省打开perfTrace
    boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
    boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, false);

    int priority = 0;
    try {
      priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
    }catch (NumberFormatException e){
      LOG.warn("prioriy set to 0, because NumberFormatException, the value is: "+System.getProperty("PROIORY"));
    }

    //初始化PerfTrace
    PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, -1, priority, traceEnable);
    perfTrace.setJobInfo(allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO), perfReportEnable, 0);

    // 实例 JobContainer 并启动
    AbstractContainer container = new JobContainer(allConf);
    container.start();
  }

  private static String filterJobConfiguration(final Configuration configuration) {
    Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();
    Configuration jobContent = jobConfWithSetting.getConfiguration("content");
    filterSensitiveConfiguration(jobContent);
    jobConfWithSetting.set("content",jobContent);
    return jobConfWithSetting.beautify();
  }

  private static Configuration filterSensitiveConfiguration(Configuration configuration){
    Set<String> keys = configuration.getKeys();
    for (final String key : keys) {
      boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password") || StringUtils.endsWithIgnoreCase(key, "accessKey");
      if (isSensitive && configuration.get(key) instanceof String) {
        configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
      }
    }
    return configuration;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = 0;
    try {
      Engine.entry(args);
    } catch (Throwable e) {
      exitCode = 1;
      LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

      if (e instanceof DataXException) {
        DataXException tempException = (DataXException) e;
        ErrorCode errorCode = tempException.getErrorCode();
        if (errorCode instanceof FrameworkErrorCode) {
          FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
          exitCode = tempErrorCode.toExitValue();
        }
      }

      System.exit(exitCode);
    }
    System.exit(exitCode);
  }
}
