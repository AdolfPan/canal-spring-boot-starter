package com.dj.boot.canal.message;


/**
 * <br>
 * <p>消息转换器</p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午5:03
 */
public interface MessageConverter extends Runnable {

    /**
     * 睡眠time
     * @param time
     */
    default void interrupt(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
