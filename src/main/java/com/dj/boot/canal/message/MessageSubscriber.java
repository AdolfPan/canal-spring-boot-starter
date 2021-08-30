package com.dj.boot.canal.message;

import com.dj.boot.canal.lang.ConsumeStatus;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.List;

/**
 * <br>
 * <p>binlog变更事件监听器</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:07
 */
@Order(Ordered.HIGHEST_PRECEDENCE + 1000)
@FunctionalInterface
public interface MessageSubscriber {

    /**
     * message subscriber
     * @param commonMessages
     */
    ConsumeStatus watch(List<CommonMessage> commonMessages);

}
