package com.dj.boot.canal.message;

import com.alibaba.otter.canal.client.CanalConnector;
import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.lang.SubscriberMetadata;
import com.dj.boot.canal.valobj.Instance;

import java.util.Map;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午5:13
 */
public class DefaultConverter extends AbstractBasicMessageTransponder {

    public DefaultConverter(CanalConnector connector, Map.Entry<String, Instance> config, Map<String, SubscriberMetadata> subscribers, CanalConfiguration configuration) {
        super(connector, config, subscribers, configuration);
    }

}