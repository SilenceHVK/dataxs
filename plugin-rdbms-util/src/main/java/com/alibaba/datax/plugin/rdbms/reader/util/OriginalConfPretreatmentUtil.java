package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class OriginalConfPretreatmentUtil {
	private static final Logger LOG = LoggerFactory.getLogger(OriginalConfPretreatmentUtil.class);

	public static DataBaseType DATABASE_TYPE;

	public static void doPretreatment(Configuration originalConfig) {
		// 检查 connections 配置（必填）
		originalConfig.getNecessaryValue(Key.CONNECTION, DBUtilErrorCode.REQUIRED_VALUE);
		dealWhere(originalConfig);
		simplifyConf(originalConfig);
	}

	public static void dealWhere(Configuration originalConfig) {
		String where = originalConfig.getString(Key.WHERE, null);
		if (StringUtils.isNotBlank(where)) {
			String whereImprove = where.trim();
			if (whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
				whereImprove = whereImprove.substring(0, whereImprove.length() - 1);
			}
			originalConfig.set(Key.WHERE, whereImprove);
		}
	}

	/**
	 * 对配置进行初步处理：
	 *
	 * <ol>
	 *   <li>处理同一个数据库配置了多个jdbcUrl的情况
	 *   <li>识别并标记是采用querySql 模式还是 table 模式
	 *   <li>对 table 模式，确定分表个数，并处理 column 转 *事项
	 * </ol>
	 */
	private static void simplifyConf(Configuration originalConfig) {
		originalConfig.set(Constant.IS_TABLE_MODE, true);
		dealJdbcAndTable(originalConfig);
	}

	private static void dealJdbcAndTable(Configuration originalConfig) {
		boolean checkSlave = originalConfig.getBool(Key.CHECK_SLAVE, false);
		boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);
		boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);

		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		List<String> preSql = originalConfig.getList(Key.PRE_SQL, String.class);
		int tableCount = 0;
		for (int i = 0, len = conns.size(); i < len; i++) {
			Configuration connConf = Configuration.from(conns.get(i).toString());
			connConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);

			// 获取 connections 的配置
			List<DataBaseType> exclude = Arrays.asList(DataBaseType.DUCKDB, DataBaseType.MySql);

			String username = exclude.contains(DATABASE_TYPE) && StringUtils.isBlank(connConf.getString(Key.USERNAME)) ? "" : connConf.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
			String password = exclude.contains(DATABASE_TYPE) && StringUtils.isBlank(connConf.getString(Key.PASSWORD)) ? "" : connConf.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);
			String jdbcUrl = connConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);

			// 设置 jdbcUrl 地址
			if (isPreCheck) {
				jdbcUrl = DBUtil.chooseJdbcUrlWithoutRetry(DATABASE_TYPE, jdbcUrl, username, password, preSql, checkSlave);
			} else {
				jdbcUrl = DBUtil.chooseJdbcUrl(DATABASE_TYPE, jdbcUrl, username, password, preSql, checkSlave);
			}
			jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);
			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);
			LOG.info("Available jdbcUrl:{}.", jdbcUrl);

			if (isTableMode) {
				// 获取所有 tables 配置
				List<JSONObject> tables = connConf.getList(Key.TABLE, JSONObject.class);
				if (tables == null || tables.isEmpty()) {
					throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
						"您所配置的读取数据库表:%s 不正确. 因为DataX根据您的配置找不到这张表. 请检查您的配置并作出修改." + "请先了解 DataX 配置.",
						StringUtils.join(tables, ",")));
				}
				tableCount = tables.size();
				originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Constant.TABLE_NUMBER_MARK), tables.size());
			}
		}
		originalConfig.set(Constant.TABLE_NUMBER_MARK, tableCount);
	}
}
