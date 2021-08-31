package com.dj.boot.canal.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.dj.boot.canal.message.CommonMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/31 下午3:46
 */
public class SqlBuildUtil {

    public static String buildInsertSql(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> columnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("insert into " + entry.getHeader().getTableName() + " (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append(columnList.get(i).getName());
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(") VALUES (");
                for (int i = 0; i < columnList.size(); i++) {
                    String str = columnList.get(i).getValue().replaceAll("'", "\"");
                    sql.append("'" + str + "'");
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                return sql.toString();
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildInsertSql(FlatMessage message) {
        try {
            StringBuffer sql = new StringBuffer("insert into " + message.getTable() + " (");
            for (Map<String, String> datum : message.getData()) {
                if (!CollectionUtils.isEmpty(datum)) {
                    List<String> cls = datum.keySet().stream().collect(Collectors.toList());
                    for (int i = 0; i < cls.size(); i++) {
                        sql.append(cls.get(i));
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    sql.append(") VALUES (");
                    for (int i = 0; i < cls.size(); i++) {
                        String str = datum.get(cls.get(i)).replaceAll("'", "\"");
                        sql.append("'" + str + "'");
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    sql.append(")");
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static String buildInsertSql(CommonMessage message) {
        try {
            StringBuffer sql = new StringBuffer("insert into " + message.getTable() + " (");
            for (Map<String, Object> datum : message.getData()) {
                if (!CollectionUtils.isEmpty(datum)) {
                    List<String> cls = datum.keySet().stream().collect(Collectors.toList());
                    for (int i = 0; i < cls.size(); i++) {
                        sql.append(cls.get(i));
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    sql.append(") VALUES (");
                    for (int i = 0; i < cls.size(); i++) {
                        String str = String.valueOf(datum.get(cls.get(i))).replaceAll("'", "\"");
                        sql.append("'" + str+ "'");
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    sql.append(")");
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildUpdateSql(FlatMessage message) {
        try {
            StringBuffer sql = new StringBuffer("update " + message.getTable() + " set ");
            for (Map<String, String> datum : message.getData()) {
                if (!CollectionUtils.isEmpty(datum)) {
                    List<String> cls = datum.keySet().stream().collect(Collectors.toList());
                    for (int i = 0; i < cls.size(); i++) {
                        String str = datum.get(cls.get(i)).replaceAll("'", "\"");
                        sql.append(" " + cls.get(i)
                                + " = '" + str + "'");
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    if (!CollectionUtils.isEmpty(message.getPkNames())) {
                        sql.append(" where ");
                        for (Map<String, String> oldDatum : message.getOld()) {
                            for (int i = 0; i < message.getPkNames().size(); i++) {
                                sql.append(message.getPkNames().get(i) + "=" + oldDatum.get(message.getPkNames().get(i)));
                                if (i != message.getPkNames().size() - 1) {
                                    sql.append(" and ");
                                }
                            }
                        }
                    }
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildUpdateSql(CommonMessage message) {
        try {
            StringBuffer sql = new StringBuffer("update " + message.getTable() + " set ");
            for (Map<String, Object> datum : message.getData()) {
                if (!CollectionUtils.isEmpty(datum)) {
                    List<String> cls = datum.keySet().stream().collect(Collectors.toList());
                    for (int i = 0; i < cls.size(); i++) {
                        String str = String.valueOf(datum.get(cls.get(i))).replaceAll("'", "\"");
                        sql.append(" " + cls.get(i)
                                + " = '" + str + "'");
                        if (i != cls.size() - 1) {
                            sql.append(",");
                        }
                    }
                    if (!CollectionUtils.isEmpty(message.getPkNames())) {
                        sql.append(" where ");
                        for (Map<String, Object> oldDatum : message.getOld()) {
                            for (int i = 0; i < message.getPkNames().size(); i++) {
                                sql.append(message.getPkNames().get(i) + "=" + oldDatum.get(message.getPkNames().get(i)));
                                if (i != message.getPkNames().size() - 1) {
                                    sql.append(" and ");
                                }
                            }
                        }
                    }
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildUpdateSql(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> newColumnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("update " + entry.getHeader().getTableName() + " set ");
                for (int i = 0; i < newColumnList.size(); i++) {
                    String str = newColumnList.get(i).getValue().replaceAll("'", "\"");
                    sql.append(" " + newColumnList.get(i).getName()
                            + " = '" + str + "'");
                    if (i != newColumnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(" where ");
                List<CanalEntry.Column> oldColumnList = rowData.getBeforeColumnsList();
                for (CanalEntry.Column column : oldColumnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                return sql.toString();
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildDeleteSql(FlatMessage message) {
        try {
            StringBuffer sql = new StringBuffer("delete from " + message.getTable() + " where ");
            for (Map<String, String> datum : message.getOld()) {
                if (CollectionUtils.isEmpty(message.getPkNames())) {
                    for (int i = 0; i < message.getPkNames().size(); i++) {
                        sql.append(message.getPkNames().get(i) + "=" + datum.get(message.getPkNames().get(i)));
                        if (i != message.getPkNames().size() - 1) {
                            sql.append(" and ");
                        }
                    }
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildDeleteSql(CommonMessage message) {
        try {
            StringBuffer sql = new StringBuffer("delete from " + message.getTable() + " where ");
            for (Map<String, Object> datum : message.getOld()) {
                if (!CollectionUtils.isEmpty(message.getPkNames())) {
                    for (int i = 0; i < message.getPkNames().size(); i++) {
                        sql.append(message.getPkNames().get(i) + "=" + datum.get(message.getPkNames().get(i)));
                        if (i != message.getPkNames().size() - 1) {
                            sql.append(" and ");
                        }
                    }
                }
            }
            return sql.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String buildDeleteSql(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> columnList = rowData.getBeforeColumnsList();
                StringBuffer sql = new StringBuffer("delete from " + entry.getHeader().getTableName() + " where ");
                for (CanalEntry.Column column : columnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                return sql.toString();
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

}