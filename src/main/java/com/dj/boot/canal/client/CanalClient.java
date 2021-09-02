package com.dj.boot.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.dj.boot.canal.annotation.CanalMessageFilter;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.message.MessageSubscriber;
import com.dj.boot.canal.utils.BootBeanFactory;
import com.dj.boot.canal.valobj.Instance;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
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
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午6:05
 */
@Slf4j
public class CanalClient extends AbstractClient {

    private ThreadPoolExecutor executor;
    protected final Map<String, SubscriberMetadata> subscriberMap = Maps.newLinkedHashMap();

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
    protected void buildConnector(CanalConnector connector, Map.Entry<String, Instance> config, CanalConfiguration configuration) {
        executor.submit(converter.initConverter(connector, config, subscriberMap, configuration));
    }

    private void initSubscriber() {
        Collection<MessageSubscriber> subscriberBeans = BootBeanFactory.getBeansByType(MessageSubscriber.class);
        if (!CollectionUtils.isEmpty(subscriberBeans)) {
            Iterator<MessageSubscriber> iterator = subscriberBeans.iterator();
            while (iterator.hasNext()) {
                MessageSubscriber subscriber = iterator.next();
                Class<? extends MessageSubscriber> clazz = subscriber.getClass();
                CanalMessageFilter filter = clazz.getAnnotation(CanalMessageFilter.class);
                SubscriberMetadata metadata = SubscriberMetadata.builder()
                        .subscriber(subscriber)
                        .build();
                if (Objects.nonNull(filter)) {
                    metadata.setInstance(filter.instance())
                            .setSchemas(Arrays.asList(filter.schemas()))
                            .setTables(Arrays.asList(filter.tables()))
                            .setEventTypes(Arrays.asList(filter.eventTypes()));
                }
                log.info(metadata.toString());
                subscriberMap.put(clazz.getName(), metadata);
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        executor.shutdown();
    }
}
