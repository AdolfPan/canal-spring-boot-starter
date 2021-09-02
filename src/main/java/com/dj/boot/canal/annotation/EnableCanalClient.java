package com.dj.boot.canal.annotation;

import com.dj.boot.canal.configure.CanalConfiguration;
import com.dj.boot.canal.configure.CanalLauncherConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * <br>
 * <p>canal enable</p>
 *
 * <br>
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午6:59
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import({CanalConfiguration.class, CanalLauncherConfiguration.class})
public @interface EnableCanalClient {
}