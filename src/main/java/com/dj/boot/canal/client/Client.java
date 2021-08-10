package com.dj.boot.canal.client;

/**
 * <br>
 * <p>canal</p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/9 下午3:43
 */
public interface Client {

    /**
     * 初始化canal客户端连接
     */
    void init();

    /**
     * 销毁
     */
    void destroy();

    /**
     * client是否运行中
     * @return
     */
    boolean isRunning();


}
