package com.dj.boot.canal.lang;

/**
 * <br>
 * <p>
 *     消费状态
 * </p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/30 下午2:46
 */
public enum ConsumeStatus {

    //消费成功
    success,
    //消费失败
    fail,
    //消息挂起（锁）
    hangup

}
