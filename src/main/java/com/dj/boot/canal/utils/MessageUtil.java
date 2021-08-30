package com.dj.boot.canal.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.message.CommonMessage;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <br>
 * <p>消息工具</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:26
 */
@Slf4j
public final class MessageUtil {

    /**
     * EXCLUDE_SCHEMA
     */
    public static final List<String> EXCLUDE_SCHEMA = Lists.newArrayList("mysql", "sys", "performance_schema", "information_schema");

    public static List<CommonMessage> convert(Message message, String schema) {
        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<CommonMessage> msgs = new ArrayList<>(entries.size());
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND
                    || EXCLUDE_SCHEMA.contains(entry.getHeader().getSchemaName())
                    || (StringUtils.isNotBlank(schema) && !StringUtils.equals(entry.getHeader().getSchemaName(), schema))) {
                continue;
            }
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final CommonMessage msg = new CommonMessage();
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setDatabase(entry.getHeader().getSchemaName());
            msg.setTable(entry.getHeader().getTableName());
            msg.setType(eventType.toString());
            msg.setExecuteTime(entry.getHeader().getExecuteTime());
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setTimeStamp(System.currentTimeMillis());
            msg.setSql(rowChange.getSql());
            msgs.add(msg);
            List<Map<String, Object>> data = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                msg.setPkNames(new ArrayList<>());
                int i = 0;
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;
                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }
                    final int index = i;
                    columns.stream().forEach(cl -> {
                        if (index == 0) {
                            if (cl.getIsKey()) {
                                msg.getPkNames().add(cl.getName());
                            }
                        }
                        row.put(cl.getName(),
                                cl.getIsNull()?
                                        null:
                                        JdbcTypeUtil.typeConvert(msg.getTable(), cl.getName(), cl.getValue(), cl.getSqlType(), cl.getMysqlType()));
                    });
                    if (!row.isEmpty()) {
                        data.add(row);
                    }
                    i++;
                }
                if (!data.isEmpty()) {
                    msg.setData(data);
                }
            }
        }
        return msgs;
    }

    public static List<CommonMessage> convert(List<FlatMessage> messages, String schema) {
        if (CollectionUtils.isEmpty(messages)) {
            return Lists.newArrayList();
        }
        return messages.stream()
                .map(msg -> convert(msg, schema))
                .filter(msg -> Objects.nonNull(msg))
                .collect(Collectors.toList());
    }

    private static CommonMessage convert(FlatMessage message, String schema) {
        if (message == null) {
            return null;
        }
        if (StringUtils.isNotBlank(schema)
                && (EXCLUDE_SCHEMA.contains(message.getTable())
                || !StringUtils.equals(message.getTable(), schema))) {
            return null;
        }
        final CommonMessage msg = new CommonMessage();
        msg.setIsDdl(message.getIsDdl());
        msg.setDatabase(message.getDatabase());
        msg.setTable(message.getTable());
        msg.setType(message.getType());
        msg.setTimeStamp(System.currentTimeMillis());
        msg.setSql(message.getSql());
        msg.setPkNames(message.getPkNames());
        List<Map<String, Object>> data = Lists.newArrayList();
        if (!message.getIsDdl()) {
            for (Map<String, String> datum : message.getData()) {
                if (!CollectionUtils.isEmpty(datum)) {
                    Iterator<String> iterator = datum.keySet().iterator();
                    while (iterator.hasNext()) {
                        String next = iterator.next();
                        Map put = Maps.newLinkedHashMap();
                        put.put(next, datum.get(next));
                        data.add(put);
                    }
                }
            }
            if (!CollectionUtils.isEmpty(data)) {
                msg.setData(data);
            }
        }
        return msg;
    }

}
