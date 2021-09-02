package com.dj.boot.canal.configure;

import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * <br>
 * <p>canal 注册启动</p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/9 下午7:00
 */
@Slf4j
@Component
public class CanalLauncher implements InitializingBean {

    @Autowired
    private CanalConfiguration canalConfiguration;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (Objects.isNull(canalConfiguration)) {
            throw new CanalClientException("CanalLauncher error. canal cfg is null.");
        }
    }


}
