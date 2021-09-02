package com.dj.boot.canal.annotation;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/10 上午8:01
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface CanalMessageFilter {

    /**
     * 接收实例
     *  [必填字段]
     * @return
     */
    String instance();

    /**
     * 库
     * @return
     */
    String[] schemas() default {};

    /**
     * 表
     * @return
     */
    String[] tables() default {};

    /**
     * sql options event
     * @return
     */
    CanalEntry.EventType[] eventTypes() default {};

}
