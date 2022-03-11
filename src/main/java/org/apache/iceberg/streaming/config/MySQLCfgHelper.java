package org.apache.iceberg.streaming.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/** Get job conf from mysql. */
public class MySQLCfgHelper {
    private static final Logger logger =  LoggerFactory.getLogger(MySQLCfgHelper.class);

    /**
     * 从 MySQL 配置库中读取作业运行配置信息.
     *
     * @param driver Driver.
     * @param url Mysql jdbc url.
     * @param user Mysql login user.
     * @param password Mysql login password.
     * @param confKey 配置在配置库中的索引Key.
     * @param andBy 作业组分隔符
     * @param orBy 表分隔符
     * @return 作业运行配置的值
     * @throws IOException IOException.
     * @throws ClassNotFoundException ClassNotFoundException.
     * @throws SQLException SQLException.
     */
    public static ArrayList<TableCfg> getConf(
            String driver, String url, String user, String password, String andBy, String orBy, String confKey)
            throws IOException, ClassNotFoundException, SQLException {
        String[] columnNames = new String[]{"batch_id", "group_id", "conf_id"};
        String condition = getCondition(columnNames, confKey, andBy, orBy);
        String prepareSQL =
                String.format(
                        "select * from `streaming`.`tbl_streaming_job_conf` where %s",
                        condition);
        logger.info(String.format("load run configure with url[%s], user[%s], prepareSQL[%s]", url, user, prepareSQL));
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, user, password);
        PreparedStatement statement = conn.prepareStatement(prepareSQL);
        ResultSet rs = statement.executeQuery();
        ArrayList<TableCfg> result = new ArrayList<TableCfg>();
        while(rs.next()){
            TableCfg tableCfg = new TableCfg();
            tableCfg.setId(rs.getString("id").trim());
            tableCfg.setBatchId(rs.getString("batch_id").trim());
            tableCfg.setGroupId(rs.getString("group_id").trim());
            tableCfg.setConfId(rs.getString("conf_id").trim());
            tableCfg.setConfValue(rs.getString("conf_value").trim());

            Timestamp createTime = rs.getTimestamp("create_time");
            Timestamp updateTime = rs.getTimestamp("update_time");

            tableCfg.setCreateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(createTime));
            tableCfg.setUpdateTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(updateTime));

            result.add(tableCfg);
        }
        rs.close();
        statement.close();
        conn.close();
        return  result;
    }


    /**
     * 获取查询 single column 值使用 OR 连接的 SQL Where 条件.
     *
     * @param columnName column name
     * @param columnValue column values, which contact by keySplitBy
     * @param orBy column value delimiter
     * @return 包含多个 column 值的 SQL Where 条件, contact by ' OR '
     */
    public static String orCase(String columnName, String columnValue, String orBy) {
        String[] caseArray = columnValue.split(orBy);
        if (caseArray.length == 1 && caseArray[0].trim().equals("")) {
            return "(1=1)";
        } else if (caseArray.length == 1) {
            return "(" + columnName.trim() + " = '" + caseArray[0].trim() + "'" + ")";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(columnName.trim()).append(" = '").append(caseArray[0].trim()).append("'");
            for (int i = 1; i < caseArray.length; i++) {
                builder.append(" OR ")
                        .append(columnName.trim())
                        .append(" = '")
                        .append(caseArray[i])
                        .append("'");
            }
            builder.append(")");
            return builder.toString();
        }
    }

    /**
     * 获取查询 multer column 值使用 AND 连接的 SQL Where 条件.
     *
     * @param columnNames column names
     * @param columnValues column values
     * @param andBy group value delimiter
     * @param orBy column value delimiter
     * @return 查询 multer column 值的 SQL Where 条件.
     */
    public static String getCondition(String[] columnNames, String columnValues, String andBy, String orBy) {
        String[] columnValueArr = columnValues.split(andBy);
        if (columnValueArr.length <= 1) {
            return orCase(columnNames[0], columnValueArr[0], orBy);
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(orCase(columnNames[0], columnValueArr[0], orBy));
            for (int i = 1; i < columnValueArr.length; i++) {
                builder.append(" AND ")
                        .append(orCase(columnNames[i], columnValueArr[i], orBy));
            }
            builder.append(")");
            return builder.toString();
        }
    }

}
