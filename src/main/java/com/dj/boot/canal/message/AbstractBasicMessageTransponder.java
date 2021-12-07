package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.utils.MessageUtil;
import com.dj.boot.canal.lang.ConsumeStatus;
import com.dj.boot.canal.valobj.Instance;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午5:43
 */
@Slf4j
public abstract class AbstractBasicMessageTransponder extends AbstractMessageConverter {

    public AbstractBasicMessageTransponder(CanalConnector connector, Map.Entry<String, Instance> config, Map<String, SubscriberMetadata> subscriberMap, CanalConfiguration configuration) {
        super(connector, config, subscriberMap, configuration);
    }

    @Override
    protected void postMsg(List<Object> messages, CanalConnector connector, long batchId, String instance, PostCall call) {
        try {
            if (CollectionUtils.isEmpty(messages)) {
                return;
            }
            if (CollectionUtils.isEmpty(subscribers)) {
                call.call(true);
                return;
            }
            if (StringUtils.isBlank(instance)) {
                call.call(true);
                return;
            }

            SubscriberMetadata subscriber = null;
            Optional<SubscriberMetadata> first = subscribers.values()
                    .stream()
                    .filter(sub -> StringUtils.equalsAnyIgnoreCase(instance, sub.getInstance()))
                    .findFirst();
            if (Objects.nonNull(first)
                    && first.isPresent()) {
                subscriber = first.get();
            }
            if (Objects.isNull(subscriber)) {
                log.warn("Instance {} no subscribe metadata information.", instance);
                call.call(true);
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

            if (!CollectionUtils.isEmpty(finalCommonMessages)) {
                List<CommonMessage> commonMessages = messageFilter(finalCommonMessages, subscriber);
                if (!CollectionUtils.isEmpty(commonMessages)) {
                    ConsumeStatus status = subscriber.getSubscriber().watch(commonMessages);
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
                }
            }
            call.call(true);
        } catch (Exception e) {
            log.error("AbstractBasicMessageTransponder postMsg process error. ex:", e);
            connector.rollback();
        }
    }

    protected List<CommonMessage> messageFilter(List<CommonMessage> finalCommonMessages, SubscriberMetadata metadata) {
        if (CollectionUtils.isEmpty(finalCommonMessages)) {
            return Lists.newArrayList();
        }
        List<String> schemas = metadata.getSchemas();
        List<String> tables = metadata.getTables();
        List<String> types = !CollectionUtils.isEmpty(metadata.getEventTypes())? metadata.getEventTypes().stream().map(type -> type.name().toLowerCase()).collect(Collectors.toList()) : null;

        List<CommonMessage> collect = finalCommonMessages.stream()
                .filter(msg -> Objects.nonNull(msg))
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(collect) && !CollectionUtils.isEmpty(schemas)) {
            collect = collect.stream()
                    .filter(msg -> !CollectionUtils.isEmpty(schemas) && schemas.contains(msg.getDatabase().toLowerCase()))
                    .collect(Collectors.toList());
        }
        if (!CollectionUtils.isEmpty(collect) && !CollectionUtils.isEmpty(tables)) {
            collect = collect.stream()
                    .filter(msg -> !CollectionUtils.isEmpty(tables) && tables.contains(msg.getTable().toLowerCase()))
                    .collect(Collectors.toList());
        }
        if (!CollectionUtils.isEmpty(collect) && !CollectionUtils.isEmpty(types)) {
            collect = collect.stream()
                    .filter(msg -> !CollectionUtils.isEmpty(types) && types.contains(msg.getType().toLowerCase()))
                    .collect(Collectors.toList());
        }

        return collect;
    }

}
