package com.dj.boot.canal.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/10 上午8:01
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface CanalMessageSubscriber {
}
