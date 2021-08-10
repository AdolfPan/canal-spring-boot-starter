package com.dj.boot.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.message.Converter;
import com.dj.boot.canal.message.DefaultConverter;
import com.dj.boot.canal.valobj.Instance;
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
import java.util.stream.Collectors;

/**
 * <br>
 * <p>客户端抽象</p>
 *
 * <br>
 *
 * @author panrusen
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
        this.converter = (connector, config, subscribers) -> new DefaultConverter(connector, config, subscribers);
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
                    buildConnector(buildEntry(instanceEntry), instanceEntry);
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
    protected abstract void buildConnector(CanalConnector connector, Map.Entry<String, Instance> config);

    /**
     * 装配连接器
     * @param instanceEntry
     * @return
     */
    private CanalConnector buildEntry(Map.Entry<String, Instance> instanceEntry) {
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

}
