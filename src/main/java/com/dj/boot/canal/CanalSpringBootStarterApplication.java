package com.dj.boot.canal;

import com.dj.boot.canal.configure.CanalConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Objects;

@Slf4j
@SpringBootApplication(scanBasePackages = {
        "com.dj.boot.canal"
})
public class CanalSpringBootStarterApplication implements CommandLineRunner {

    @Autowired
    private CanalConfiguration canalConfiguration;


    public static void main(String[] args) {
        SpringApplication.run(CanalSpringBootStarterApplication.class, args);
    }


//    @Autowired
//    private TestClient testClient;

    @Override
    public void run(String... args) throws Exception {
//        log.info("Ci::{}", testClient);
        if (Objects.nonNull(canalConfiguration)) {
        }
    }
}

