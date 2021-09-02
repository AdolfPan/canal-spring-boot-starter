package com.dj.boot.canal.configure;

import com.dj.boot.canal.valobj.Instance;
import com.dj.boot.canal.valobj.KafkaConfig;
import com.dj.boot.canal.valobj.RocketMQConfig;
import com.dj.boot.canal.valobj.ServerMode;
import com.google.common.collect.Maps;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * <br>
 * <p>canal instances config</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/6 下午5:06
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "dj.canal")
public class CanalConfiguration {

    /**
     * canal server mode (tcp kafka rocketMQ)
     */
    private String mode = ServerMode.tcp.name();

    /**
     * rocket mq cfg
     */
    private RocketMQConfig mqConfig;

    /**
     * kafka cfg
     */
    private KafkaConfig kafkaConfig;

    private Map<String, Instance> instances = Maps.newLinkedHashMap();

}
