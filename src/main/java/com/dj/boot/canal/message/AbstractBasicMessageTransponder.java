package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.utils.MessageUtil;
import com.dj.boot.canal.lang.ConsumeStatus;
import com.dj.boot.canal.valobj.Instance;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * <br>
 * <p>
 *     消息转换器
 *     向订阅端提交消息并获取return后提交ack
 * </p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:43
 */
@Slf4j
public abstract class AbstractBasicMessageTransponder extends AbstractMessageConverter {

    public AbstractBasicMessageTransponder(CanalConnector connector, Map.Entry<String, Instance> config, Map<String, SubscriberMetadata> subscriberMap) {
        super(connector, config, subscriberMap);
    }

    @Override
    protected void postMsg(List<Object> messages, CanalConnector connector, long batchId) {
        try {
            if (CollectionUtils.isEmpty(messages)) {
                log.warn("message is empty.");
                return;
            }
            List<CommonMessage> finalCommonMessages = Lists.newArrayList();
            messages.stream().forEach(msg -> {
                if (msg instanceof Message) {
                    finalCommonMessages.addAll(MessageUtil.convert((Message)msg));
                } else if (msg instanceof FlatMessage) {
                    finalCommonMessages.add(MessageUtil.convert((FlatMessage)msg));
                }
            });

            if (!CollectionUtils.isEmpty(subscribers)
                    && !CollectionUtils.isEmpty(finalCommonMessages)) {
                subscribers.values().stream().forEach(listener -> {
                    List<CommonMessage> commonMessages = messageFilter(finalCommonMessages, listener);
                    MessageSubscriber subscriber = listener.getSubscriber();
                    ConsumeStatus status = subscriber.watch(commonMessages);
                    if (Objects.equals(ConsumeStatus.success, status)) {
                        // mq提交确认
                        if (connector instanceof RocketMQCanalConnector) {
                            ((RocketMQCanalConnector) connector).ack();
                        }
                        // TCP提交确认
                        if (batchId != -1) {
                            connector.ack(batchId);
                        }
                    }
                });
            }
        } catch (Exception e) {
            log.error("AbstractBasicMessageTransponder postMsg process error. ex:", e);
            connector.rollback();
        }
    }

    protected List<CommonMessage> messageFilter(List<CommonMessage> finalCommonMessages, SubscriberMetadata metadata) {
        if (CollectionUtils.isEmpty(finalCommonMessages)) {
            return Lists.newArrayList();
        }
        List<String> tables = Objects.nonNull(metadata.getTables()) && metadata.getTables().length > 0? Arrays.asList(metadata.getTables()): null;
        List<String> schemas = Objects.nonNull(metadata.getTables()) && metadata.getSchemas().length > 0? Arrays.asList(metadata.getSchemas()): null;
        List<CanalEntry.EventType> eventTypes = Objects.nonNull(metadata.getTables()) && metadata.getEventTypes().length > 0? Arrays.asList(metadata.getEventTypes()): null;
        List<String> types = !CollectionUtils.isEmpty(eventTypes)? eventTypes.stream().map(eventType -> eventType.name().toLowerCase()).collect(Collectors.toList()) : null;

        List<CommonMessage> collect = finalCommonMessages.stream()
                .filter(msg -> (CollectionUtils.isEmpty(schemas) || (!CollectionUtils.isEmpty(schemas) && schemas.contains(msg.getDatabase())))
                        || (CollectionUtils.isEmpty(tables) || (!CollectionUtils.isEmpty(tables) && tables.contains(msg.getTable())))
                        || (CollectionUtils.isEmpty(types) || (!CollectionUtils.isEmpty(types) && types.contains(msg.getType()))))
                .collect(Collectors.toList());
        return collect;
    }

}
