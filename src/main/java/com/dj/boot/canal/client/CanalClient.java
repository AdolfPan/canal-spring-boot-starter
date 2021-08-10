package com.dj.boot.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.message.MessageSubscriber;
import com.dj.boot.canal.utils.BootBeanFactory;
import com.dj.boot.canal.valobj.Instance;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <br>
 * <p>canal client</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午6:05
 */
public class CanalClient extends AbstractClient {

    private ThreadPoolExecutor executor;
    protected final List<MessageSubscriber> subscribers = new ArrayList<>();

    public CanalClient(CanalConfiguration canalConfiguration) {
        super(canalConfiguration);
        executor =  new ThreadPoolExecutor(
                5,
                20,
                120L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                Executors.defaultThreadFactory());
        initSubscriber();
    }

    @Override
    protected void buildConnector(CanalConnector connector, Map.Entry<String, Instance> config) {
        executor.submit(converter.initConverter(connector, config, subscribers));
    }

    private void initSubscriber() {
        Collection<MessageSubscriber> subscriberBeans = BootBeanFactory.getBeansByType(MessageSubscriber.class);
        if (!CollectionUtils.isEmpty(subscriberBeans)) {
            subscribers.addAll(subscriberBeans);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        executor.shutdown();
    }
}
