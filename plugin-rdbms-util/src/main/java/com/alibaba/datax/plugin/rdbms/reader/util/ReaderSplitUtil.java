package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class ReaderSplitUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderSplitUtil.class);

  public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber) {
    boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
    Integer fetchSize = originalSliceConfig.getInt(Constant.FETCH_SIZE);
    int eachTableShouldSplittedNumber = -1;
    if (isTableMode) {
      // adviceNumber这里是channel数量大小, 即datax并发task数量
      // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
      eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
    }

    String where = originalSliceConfig.getString(Key.WHERE, null);
    String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);
    List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);
    List<Configuration> splittedConfigs = new ArrayList<Configuration>();

    for (int i = 0, len = conns.size(); i < len; i++) {
      Configuration sliceConfig = Configuration.from(conns.get(i).toString()).clone();
      // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
      String jdbcUrl = sliceConfig.getString(Key.JDBC_URL);
      sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));
      List<JSONObject> tables = sliceConfig.getList(Key.TABLE, JSONObject.class);
      Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

      // 最终切分份数不一定等于 eachTableShouldSplittedNumber
      boolean needSplitTable = eachTableShouldSplittedNumber > 1 && StringUtils.isNotBlank(splitPk);
      if (needSplitTable && tables.size() == 1) {
        Integer splitFactor = originalSliceConfig.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
      }

      // 对 table 拆分
      for(JSONObject table : tables) {
        String tableName = TableExpandUtil.expandTableConf(table.getString(Key.TABLE_NAME));
        List<String> columns = JSONObject.parseArray(table.getString(Key.COLUMN), String.class);

        Configuration tempSlice = sliceConfig.clone();
        tempSlice.set(Key.TABLE, tableName);
        tempSlice.set(Key.COLUMN, columns);
        tempSlice.set(Key.WHERE, where);
        tempSlice.set(Key.SPLIT_PK, splitPk);
        tempSlice.set(Constant.FETCH_SIZE, fetchSize);

        // 判断数据表是否要拆分
        if (needSplitTable) {
            List<Configuration> splittedSlices = SingleTableSplitUtil.splitSingleTable(tempSlice, eachTableShouldSplittedNumber);
            splittedConfigs.addAll(splittedSlices);
        } else {
            String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, tableName, StringUtils.join(columns, ","));
            tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, tableName, where));
            splittedConfigs.add(tempSlice);
        }
      }
    }
    return splittedConfigs;
  }

  public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
    Configuration queryConfig = originalSliceConfig.clone();
    String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
    String where = originalSliceConfig.getString(Key.WHERE, null);

    List<Object> conns = queryConfig.getList(Constant.CONN_MARK, Object.class);

    for (int i = 0, len = conns.size(); i < len; i++){
      Configuration connConf = Configuration.from(conns.get(i).toString());
      String connPath = String.format("connection[%d]",i);
      List<String> querys = new ArrayList<>();
      List<String> splitPkQuerys = new ArrayList<>();

      // table 处理
      List<JSONObject> tables = connConf.getList(Key.TABLE, JSONObject.class);
      Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
      for (int j = 0; j <tables.size(); j++) {
        JSONObject table = tables.get(j);
        String tableName = table.get(Key.TABLE_NAME).toString();
        Object[] column = table.getJSONArray(Key.COLUMN).toArray();
        // 设置查询 sql
        querys.add(SingleTableSplitUtil.buildQuerySql(StringUtils.join(column, ","), tableName, where));
        // 设置主键拆分查询
        if (splitPK != null && !splitPK.isEmpty()){
          splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(), tableName, where));
        }
      }

      if (!splitPkQuerys.isEmpty()){
        connConf.set(Key.SPLIT_PK_SQL,splitPkQuerys);
      }
      connConf.set(Key.QUERY_SQL,querys);
      queryConfig.set(connPath,connConf);
    }
    return queryConfig;
  }

  private static int calculateEachTableShouldSplittedNumber(int adviceNumber, int tableNumber) {
    double tempNum = 1.0 * adviceNumber / tableNumber;
    return (int) Math.ceil(tempNum);
  }
}

