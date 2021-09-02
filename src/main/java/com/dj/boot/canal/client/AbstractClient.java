package com.dj.boot.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.message.Converter;
import com.dj.boot.canal.message.DefaultConverter;
import com.dj.boot.canal.valobj.Instance;
import com.dj.boot.canal.valobj.RocketMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <br>
 * <p>客户端抽象</p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午3:47
 */
@Slf4j
public abstract class AbstractClient implements Client {

    private volatile boolean running;
    private CanalConfiguration canalConfiguration;
    protected final Converter converter;

    public AbstractClient(CanalConfiguration canalConfiguration) {
        Assert.isTrue(Objects.nonNull(canalConfiguration), "canal配置信息不能为空");
        this.canalConfiguration = canalConfiguration;
        this.converter = (connector, config, subscribers, configuration) -> new DefaultConverter(connector, config, subscribers, configuration);
    }

    @Override
    public void init() {
        CanalConfiguration config = canalConfiguration;
        Map<String, Instance> instanceMap;
        if (config != null && (instanceMap = config.getInstances()) != null && !instanceMap.isEmpty()) {
            instanceMap = config.getInstances();
        } else {
            throw new CanalClientException("无法解析配置信息");
        }
        //构建连接
        instanceMap.entrySet()
                .stream()
                .forEach(instanceEntry -> {
                    log.info("canal client cfg::{}", instanceEntry);
                    buildConnector(buildEntry(instanceEntry), instanceEntry, canalConfiguration);
                });
    }

    @Override
    public void destroy() {
        this.running = false;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    /**
     * 构建client -> server 连接
     * @param connector
     * @param config
     */
    protected abstract void buildConnector(CanalConnector connector, Map.Entry<String, Instance> config, CanalConfiguration canalConfiguration);

    /**
     * 装配连接器
     * @param instanceEntry
     * @return
     */
    private CanalConnector buildEntry(Map.Entry<String, Instance> instanceEntry) {
        return process(instanceEntry);
    }

    private CanalConnector process(Map.Entry<String, Instance> instanceEntry) {
        CanalConnector canalConnector = null;
        final String mode = canalConfiguration.getMode();
        switch (mode) {
            case "tcp":
                canalConnector = buildOfTcp(instanceEntry);
                break;
            case "rocketMQ":
                canalConnector = buildOfRocketMQ(instanceEntry);
                break;
            default:
                break;
        }
        return canalConnector;
    }

    private CanalConnector buildOfTcp(Map.Entry<String, Instance> instanceEntry) {
        Instance instance = instanceEntry.getValue();
        CanalConnector connector = null;
        if (StringUtils.isNotBlank(instance.getHost())) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(instance.getHost(), Integer.parseInt(instance.getPort()));
            connector = CanalConnectors.newSingleConnector(inetSocketAddress, instanceEntry.getKey(), instance.getUserName(), instance.getPassword());
        } else {
            if (!CollectionUtils.isEmpty(instance.getZookeeperAddress())) {
                List<SocketAddress> addresses = new ArrayList<>();
                for (String s : instance.getZookeeperAddress()) {
                    String[] entry = s.split(":");
                    if (entry.length != 2) {
                        throw new CanalClientException("zk server address format is wrong. It should be 'ip:port', but the current is" + s);
                    }
                    addresses.add(new InetSocketAddress(entry[0], Integer.parseInt(entry[1])));
                }
                connector = CanalConnectors.newClusterConnector(addresses, instanceEntry.getKey(), instance.getUserName(), instance.getPassword());
            }
        }
        if (Objects.nonNull(connector)) {
            connector.connect();
            connector.subscribe(instance.getFilter());
            connector.rollback();
            return connector;
        }
        return null;
    }

    private RocketMQCanalConnector buildOfRocketMQ(Map.Entry<String, Instance> instanceEntry) {
        Instance instance = instanceEntry.getValue();
        RocketMQConfig mqConfig = canalConfiguration.getMqConfig();
        RocketMQCanalConnector connector = new RocketMQCanalConnector(
                mqConfig.getNameServers(),
                instanceEntry.getKey(),
                instance.getGroupId(),
                mqConfig.getAccessKey(),
                mqConfig.getSecretKey(),
                -1,
                mqConfig.isFlat(),
                mqConfig.isTrace(),
                null,
                mqConfig.getAccessChannel(),
                mqConfig.getNamespace());

        if (Objects.nonNull(connector)) {
            connector.connect();
            connector.subscribe();
            return connector;
        }
        return connector;
    }

}
