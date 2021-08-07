package com.dj.boot.canal.valobj;

import lombok.Data;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <br>
 * <p>canal配置信息</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/6 下午4:08
 */
@Data
public class Instance {

    /**
     * 是否开启集群模式
     */
    private boolean clusterEnabled;

    /**
     * zookeeper地址
     */
    private Set<String> zookeeperAddress = new LinkedHashSet<>();

    /**
     * canal server host address
     * 默认是本地的环回地址
     */
    private String host = "127.1.1.1";

    /**
     * canal server port
     * 默认 11111
     */
    private int port = 11111;

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
    private String filter;

    /**
     * 当错误发生时，重试次数
     */
    private int retryTimes = 5;

    /**
     * 信息捕获心跳时间
     * 单位：毫秒
     */
    private long acquireInterval = 1000;


}
