package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.valobj.Instance;
import com.dj.boot.canal.valobj.RocketMQConfig;
import com.dj.boot.canal.valobj.ServerMode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

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
    protected final Map<String, SubscriberMetadata> subscribers = Maps.newLinkedHashMap();
    private volatile boolean running = true;

    public AbstractMessageConverter(CanalConnector connector, Map.Entry<String, Instance> config, Map<String, SubscriberMetadata> subscriberMap) {
        this.connector = connector;
        this.config = config.getValue();
        this.destination = config.getKey();
        if (!CollectionUtils.isEmpty(subscriberMap)) {
            this.subscribers.putAll(subscriberMap);
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
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                Message message = connector.getWithoutAck(config.getBatchSize());
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    Thread.sleep(config.getHeartbeatInterval());
                } else {
                    postMsg(Lists.newArrayList(message), connector, batchId);
                }
            }
        } catch (CanalClientException | InterruptedException e) {
            e.printStackTrace();
            log.error("ProcessRocketMQ processTcp error. ex: ", e);
        }
    }

    private void processRocketMQ(Instance config, RocketMQCanalConnector connector) {
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                RocketMQConfig mqConfig = config.getMqConfig();
                if (mqConfig.isFlat()) {
                    List<FlatMessage> flatListWithoutAck = connector.getFlatListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                    if (CollectionUtils.isEmpty(flatListWithoutAck)) {
                        Thread.sleep(config.getHeartbeatInterval());
                    }
                    postMsg(Lists.newArrayList(flatListWithoutAck), connector, -1);
                    return;
                }

                List<Message> messages = connector.getListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                if (CollectionUtils.isEmpty(messages)) {
                    Thread.sleep(config.getHeartbeatInterval());
                }
                postMsg(Lists.newArrayList(messages), connector, -1);
            }
        } catch (CanalClientException | InterruptedException e) {
            log.error("ProcessRocketMQ processRocketMQ error. ex: ", e);
        }
    }

    /**
     * 提交
     * @param message
     * @param connector
     * @param batchId
     */
    protected abstract void postMsg(List<Object> message, CanalConnector connector, long batchId);

    /**
     * 停止
     */
    void stop() {
        running = false;
    }
}
