package com.dj.boot.canal.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author mason
 */
@Setter
@Getter
public class CommonMessage implements Serializable {

    private static final long serialVersionUID = -271881875159015958L;

    /**
     * 数据库或schema
     */
    private String database;
    /**
     * 表名
     */
    private String table;
    /**
     *
     */
    private List<String> pkNames;
    /**
     *
     */
    private Boolean isDdl;
    /**
     * 类型:INSERT/UPDATE/DELETE
     */
    private String type;
    /**
     * binlog executeTime, 执行耗时
     */
    private Long executeTime;
    /**
     * dml build timeStamp, 同步时间
     */
    private Long timeStamp;
    /**
     * 执行的sql,dml sql为空
     */
    private String sql;
    /**
     * 数据列表
     */
    private List<Map<String, Object>> data;
    private List<Map<String, Object>> old;

    public void clear() {
        database = null;
        table = null;
        type = null;
        timeStamp = null;
        executeTime = null;
        data = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "CommonMessage{" + "database='" + database + '\'' + ", table='" + table + '\'' + ", pkNames=" + pkNames
               + ", isDdl=" + isDdl + ", type='" + type + '\'' + ", es=" + executeTime + ", ts=" + timeStamp + ", sql='" + sql + '\''
               + ", data=" + data + '}';
    }
}
