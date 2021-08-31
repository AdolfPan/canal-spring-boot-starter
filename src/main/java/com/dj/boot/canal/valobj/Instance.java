package com.dj.boot.canal.valobj;

import lombok.*;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <br>
 * <p>canal配置信息</p>
 *
 * <br>
 * @author panrusen
 * @version 1.0
 * @date 2021/8/6 下午4:08
 */
@Data
public class Instance implements Serializable {

    /**
     * 接收模式
     * 支持tcp kafka rocketMQ
     */
    private String mode = "tcp";

    /**
     * zookeeper地址
     */
    private Set<String> zookeeperAddress = new LinkedHashSet<>();

    /**
     * canal server host address
     */
    private String host = "127.0.0.1";

    /**
     * canal server port
     * 默认 11111
     */
    private String port = "11111";

    /**
     * 集群--设置的用户名
     */
    private String userName = "";

    /**
     * 集群--设置的密码
     */
    private String password = "";

    /**
     * 单批次（批量）从 canal server dump数据的最多数目
     */
    private int batchSize = 1000;

    /**
     * 是否有过滤规则
     */
    private String filter = ".*\\..*";

    /**
     * 当错误发生时，重试次数
     */
    private int retryTimes = 5;

    /**
     * 心跳间隔 单位：毫秒
     */
    private long heartbeatInterval = 1000;

    /**
     * rocket mq cfg
     */
    private RocketMQConfig mqConfig;

    /**
     * kafka cfg
     */
    private KafkaConfig kafkaConfig;


}
