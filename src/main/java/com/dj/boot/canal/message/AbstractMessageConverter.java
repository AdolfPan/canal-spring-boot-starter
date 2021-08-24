package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dj.boot.canal.valobj.Instance;
import com.dj.boot.canal.valobj.ServerMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        final String threadName = Thread.currentThread().getName();
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                process(config, connector);
            }
            if (StringUtils.equals(ServerMode.rocketMQ.name(), config.getMode())) {
                connector.unsubscribe();
            }
            this.stop();
            log.info("{}::canal client stop.", threadName);
        } catch (CanalClientException e) {
            log.warn("AbstractMessageConverter CanalClientException, e: ", e);
            errorCount--;
            log.error(threadName + " error::", e);
            try {
                Thread.sleep(config.getHeartbeatInterval());
            } catch (InterruptedException ex) {
                errorCount = 0;
            }
        } catch (InterruptedException e) {
            errorCount = 0;
            connector.rollback();
        } finally {
            if (errorCount <= 0) {
                this.stop();
                log.info("{}: canal client stop.", Thread.currentThread().getName());
            }
        }
    }

    private void process(Instance config, CanalConnector connector) throws InterruptedException {
        String mode = config.getMode();
        switch (mode) {
            case "rocketMQ":
                processRocketMQ(config, (RocketMQCanalConnector) connector);
                break;
            case "tcp":
                processTcp(config, connector);
                break;
            default:
                break;
        }
    }

    private void processTcp(Instance config, CanalConnector connector) throws InterruptedException {
        Message message = connector.getWithoutAck(config.getBatchSize());
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            Thread.sleep(config.getHeartbeatInterval());
        } else {
            postMsg(message);
        }
        connector.ack(batchId);
    }

    private void processRocketMQ(Instance config, RocketMQCanalConnector connector) {
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                List<Message> msgs = connector.getListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                if (!CollectionUtils.isEmpty(msgs)) {
                    for (Message msg : msgs) {
                        long batchId = msg.getId();
                        int size = msg.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            Thread.sleep(config.getHeartbeatInterval());
                        } else {
                            postMsg(msg);
                        }
                    }
                }
                connector.ack();
            }
        } catch (CanalClientException | InterruptedException e) {
            log.warn("ProcessRocketMQ:Ex: ", e);
        }
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
