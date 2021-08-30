package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.utils.MessageUtil;
import com.dj.boot.canal.lang.ConsumeStatus;
import com.dj.boot.canal.valobj.Instance;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public AbstractBasicMessageTransponder(CanalConnector connector, Map.Entry<String, Instance> config, List<MessageSubscriber> subscribers) {
        super(connector, config, subscribers);
    }

    @Override
    protected void postMsg(Object message, CanalConnector connector) {
        try {
            if (Objects.isNull(message)) {
                log.warn("message is empty.");
            }
            List<CommonMessage> commonMessages = Lists.newArrayList();
            long batchId = -1;
            if (message instanceof Message) {
                commonMessages = MessageUtil.convert((Message)message, config.getSchema());
                batchId = ((Message)message).getId();
            }
            if (message instanceof List) {
                commonMessages = MessageUtil.convert((List<FlatMessage>) message, config.getSchema());
            }
            if (!CollectionUtils.isEmpty(subscribers)
                    && !CollectionUtils.isEmpty(commonMessages)) {
                List<CommonMessage> finalCommonMessages = commonMessages;
                long finalBatchId = batchId;
                subscribers.stream().forEach(listener -> {
                    ConsumeStatus status = listener.watch(finalCommonMessages);
                    if (Objects.equals(ConsumeStatus.success, status)) {
                        // mq提交确认
                        if (connector instanceof RocketMQCanalConnector) {
                            ((RocketMQCanalConnector) connector).ack();
                            return;
                        }
                        // TCP提交确认
                        connector.ack(finalBatchId);
                    }
                });
            }
        } catch (Exception e) {
            log.error("AbstractBasicMessageTransponder postMsg process error. ex:", e);
            connector.rollback();
        }




    }
}
