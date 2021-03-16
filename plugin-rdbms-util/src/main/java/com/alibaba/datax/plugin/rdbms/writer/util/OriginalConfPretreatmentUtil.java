package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

//    public static void doPretreatment(Configuration originalConfig) {
//        doPretreatment(originalConfig,null);
//    }

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType) {
        originalConfig.getNecessaryValue(Constant.CONN_MARK, DBUtilErrorCode.REQUIRED_VALUE);
        doCheckBatchSize(originalConfig);
        simplifyConf(originalConfig);
        dealColumnConf(originalConfig);
    }

    public static void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }
        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,Object.class);
        int tableCount = 0;
        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);
            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }
            tableCount = tables.size();
            originalConfig.set(String.format("%s[%d].%s",Constant.CONN_MARK, i, Constant.TABLE_NUMBER_MARK), tables.size());
        }
        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableCount);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable, List<String> userConfiguredColumns, int connectionIndex, int tableIndex) {
        boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
        List<String> allColumns;
        if (isPreCheck){
            allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
        } else {
            allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
        }
        LOG.info("table:[{}] all columns:[\n{}\n].", oneTable, StringUtils.join(allColumns, ","));

        if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
            LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");
            // 回填其值，需要以 String 的方式转交后续处理
            originalConfig.set(String.format("%s[%d].%s[%d].%s",Constant.CONN_MARK, connectionIndex, Key.TABLE, tableIndex, Key.COLUMN), allColumns);
        } else if (userConfiguredColumns.size() > allColumns.size()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                            userConfiguredColumns.size(), allColumns.size()));
        } else {
            // 确保用户配置的 column 不重复
            ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);
            // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
            DBUtil.getColumnMetaData(connectionFactory.getConnecttion(), oneTable,StringUtils.join(userConfiguredColumns, ","));
        }
    }

    public static void dealColumnConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK, Object.class);
        for (int i = 0, len = connections.size(); i < len; i++){
            Configuration connConf = Configuration.from(connections.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            String username = connConf.getString(Key.USERNAME);
            String password = connConf.getString(Key.PASSWORD);
            List<JSONObject> tabls = connConf.getList(Key.TABLE, JSONObject.class);

            // 构建 JDBC 连接工厂
            JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
            for (int j = 0; j < tabls.size(); j++) {
                JSONObject table = tabls.get(j);
                List<String> column = JSONObject.parseArray(table.getString(Key.COLUMN), String.class);
                dealColumnConf(originalConfig, jdbcConnectionFactory, table.getString(Key.TABLE_NAME), column, i, j);
                dealWriteMode(originalConfig, i, j, jdbcUrl, DATABASE_TYPE);
            }
        }
    }

    public static void dealWriteMode(Configuration originalConfig, int connectionIndex, int tableIndex, String jdbcUrl,DataBaseType dataBaseType) {
        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");
        List<String> columns = originalConfig.getList(String.format("%s[%d].%s[%d].%s", Constant.CONN_MARK, connectionIndex, Key.TABLE, tableIndex, Key.COLUMN), String.class);
        List<String> valueHolders = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }
        // 是否使用强制更新
        boolean forceUseUpdate = false;
        //ob10的处理
        if (dataBaseType == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }
        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode,dataBaseType, forceUseUpdate);
        originalConfig.set(String.format("%s[%d].%s[%d].%s",Constant.CONN_MARK, connectionIndex, Key.TABLE, tableIndex, Constant.INSERT_OR_REPLACE_TEMPLATE_MARK), writeDataSqlTemplate);
        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);
    }


    public static boolean isOB10(String jdbcUrl) {
        //ob10的处理
        if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            return true;
        }
        return false;
    }

}
