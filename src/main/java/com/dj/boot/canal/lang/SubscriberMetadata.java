package com.dj.boot.canal.lang;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.dj.boot.canal.message.MessageSubscriber;
import com.google.common.collect.Lists;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * <br>
 * <p>
 *     订阅者元数据信息
 * </p>
 *
 * <br>
 * @author mason
 * @version 1.0
 * @date 2021/8/31 上午9:18
 */
@Setter
@Getter
@Builder
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
public class SubscriberMetadata implements Serializable {
    private static final long serialVersionUID = 959237975446391610L;

    private MessageSubscriber subscriber;
    private String instance;
    private List<String> schemas = Lists.newArrayList();
    private List<String> tables = Lists.newArrayList();
    private List<CanalEntry.EventType> eventTypes = Lists.newArrayList();

    @Override
    public String toString() {
        return "SubscriberMetadata::schemas:" + schemas.stream().collect(Collectors.joining(","))
                + ", tables:" + tables.stream().collect(Collectors.joining(","))
                + ", eventTypes:" + eventTypes.stream().filter(type -> Objects.nonNull(type)).map(type -> type.name()).collect(Collectors.joining(","));
    }
}
