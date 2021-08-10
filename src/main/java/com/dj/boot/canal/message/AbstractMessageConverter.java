package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.dj.boot.canal.valobj.Instance;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:15
 */
@Slf4j
public abstract class AbstractMessageConverter implements MessageConverter {

    private final CanalConnector connector;
    protected final Instance config;
    protected final String destination;
    protected final List<MessageSubscriber> subscribers = new ArrayList<>();
    private volatile boolean running = true;

    public AbstractMessageConverter(CanalConnector connector, Map.Entry<String, Instance> config, List<MessageSubscriber> subscribers) {
        this.connector = connector;
        this.config = config.getValue();
        this.destination = config.getKey();
        if (subscribers != null) {
            this.subscribers.addAll(subscribers);
        }
    }

    @Override
    public void run() {
        int errorCount = config.getRetryTimes();
        final long interval = config.getHeartbeatInterval();
        final String threadName = Thread.currentThread().getName();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = connector.getWithoutAck(config.getBatchSize());
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    Thread.sleep(interval);
                } else {
                    postMsg(message);
                }
                connector.ack(batchId);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop();
        log.info("{}::canal client stop.", threadName);
    }

    /**
     * 提交
     * @param message
     */
    protected abstract void postMsg(Message message);

    /**
     * 停止
     */
    void stop() {
        running = false;
    }
}
