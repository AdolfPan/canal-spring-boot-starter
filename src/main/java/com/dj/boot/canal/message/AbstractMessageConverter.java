package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.valobj.Instance;
import com.dj.boot.canal.valobj.KafkaConfig;
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
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午5:15
 */
@Slf4j
public abstract class AbstractMessageConverter implements MessageConverter {

    private final CanalConnector connector;
    protected final Instance config;
    protected final String destination;
    protected final String mode;
    protected final RocketMQConfig mqConfig;
    protected final KafkaConfig kafkaConfig;
    protected final Map<String, SubscriberMetadata> subscribers = Maps.newLinkedHashMap();
    private volatile boolean running = true;

    public AbstractMessageConverter(CanalConnector connector, Map.Entry<String, Instance> config, Map<String, SubscriberMetadata> subscriberMap, CanalConfiguration configuration) {
        this.connector = connector;
        this.config = config.getValue();
        this.destination = config.getKey();
        this.mode = configuration.getMode();
        this.mqConfig = configuration.getMqConfig();
        this.kafkaConfig = configuration.getKafkaConfig();
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
            if (StringUtils.equals(ServerMode.rocketMQ.name(), this.mode)) {
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
        String mode = this.mode;
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
                    postMsg(Lists.newArrayList(message), connector, batchId, this.destination, (rst) -> {
                        if (rst) { connector.ack(batchId); }
                    });
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
                RocketMQConfig mqConfig = this.mqConfig;
                if (mqConfig.isFlat()) {
                    List<FlatMessage> flatListWithoutAck = connector.getFlatListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                    if (CollectionUtils.isEmpty(flatListWithoutAck)) {
                        Thread.sleep(config.getHeartbeatInterval());
                    }
                    postMsg(Lists.newArrayList(flatListWithoutAck), connector, -1, this.destination, (rst) -> {
                        if (rst) {connector.ack(); }
                    });
                    return;
                }

                List<Message> messages = connector.getListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                if (CollectionUtils.isEmpty(messages)) {
                    Thread.sleep(config.getHeartbeatInterval());
                }
                postMsg(Lists.newArrayList(messages), connector, -1, this.destination, (rst) -> {
                    if (rst) {connector.ack(); }
                });
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
    protected abstract void postMsg(List<Object> message, CanalConnector connector, long batchId, String instance, PostCall call);

    /**
     * 停止
     */
    void stop() {
        running = false;
    }

}
