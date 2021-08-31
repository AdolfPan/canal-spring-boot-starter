package com.dj.boot.canal.message;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/31 上午11:53
 */
@FunctionalInterface
public interface PostCall{
    void call(boolean rst);
}