package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.dj.boot.canal.valobj.Instance;

import java.util.List;
import java.util.Map;

/**
 * <br>
 * <p>转换器</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:01
 */
@FunctionalInterface
public interface Converter {

    /**
     * 初始化转化器
     * @param connector
     * @param config
     * @param subscribers
     * @return
     */
    MessageConverter initConverter(CanalConnector connector, Map.Entry<String, Instance> config, List<MessageSubscriber> subscribers);

}
