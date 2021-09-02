package com.dj.boot.canal.configure;

import com.dj.boot.canal.client.CanalClient;
import com.dj.boot.canal.client.Client;
import com.dj.boot.canal.utils.BootBeanFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * <br>
 * <p>client instance cfg</p>
 *
 * <br>
 *
 * @author mason
 * @version 1.0
 * @date 2021/8/10 上午8:31
 */
@Slf4j
public class CanalLauncherConfiguration {

    @Autowired
    private CanalConfiguration canalConfiguration;

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public BootBeanFactory bootBeanFactory() {
        return new BootBeanFactory();
    }

    @Bean
    private Client canalClient() {
        Client canalClient = new CanalClient(canalConfiguration);
        canalClient.init();
        return canalClient;
    }

}
