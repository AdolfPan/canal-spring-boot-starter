package com.dj.boot.canal.lang;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.dj.boot.canal.message.MessageSubscriber;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.Serializable;

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
    private String[] schemas = {};
    private String[] tables = {};
    private CanalEntry.EventType[] eventTypes = {};

    @Override
    public String toString() {
        return "SubscriberMetadata::schemas:" + schemas.toString()
                + ", tables:" + tables.toString()
                + ", eventTypes:" + eventTypes.toString();
    }
}
