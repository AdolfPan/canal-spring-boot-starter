package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.utils.MessageUtil;
import com.dj.boot.canal.valobj.Instance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

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
    protected void postMsg(Message message) {
        if (Objects.isNull(message)) {
            log.warn("message is empty.");
        }
        List<CommonMessage> commonMessages = MessageUtil.convert(message, config.getSchema());
        if (!CollectionUtils.isEmpty(subscribers)) {
            subscribers.stream().forEach(listener -> {
                listener.watch(commonMessages);
            });
        }
    }
}
