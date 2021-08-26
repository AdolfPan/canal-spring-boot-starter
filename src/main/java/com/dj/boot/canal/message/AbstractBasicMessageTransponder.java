package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.utils.MessageUtil;
import com.dj.boot.canal.valobj.Instance;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <br>
 * <p></p>
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
    protected void postMsg(Serializable message) {
        if (Objects.isNull(message)) {
            log.warn("message is empty.");
        }
        List<CommonMessage> commonMessages = Lists.newArrayList();
        if (message instanceof Message) {
            commonMessages = MessageUtil.convert((Message)message, config.getSchema());
        }
        if (message instanceof FlatMessage) {
            commonMessages = MessageUtil.convert((FlatMessage)message, config.getSchema());
        }
        if (!CollectionUtils.isEmpty(subscribers)
                && !CollectionUtils.isEmpty(commonMessages)) {
            List<CommonMessage> finalCommonMessages = commonMessages;
            subscribers.stream().forEach(listener -> {
                listener.watch(finalCommonMessages);
            });
        }
    }
}
