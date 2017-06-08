package com.fuxg;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.fuxg.canal.CanalClient;
import com.fuxg.handler.KafkaHandlerListener;
import com.fuxg.util.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
public class CanalKafkaApplication implements CommandLineRunner {

    private static Logger log = LoggerFactory.getLogger(CanalKafkaApplication.class);

    @Autowired
    ApplicationConfig applicationConfig;

    public static void main(String[] args) {
        SpringApplication.run(CanalKafkaApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        Map<String, String> canalConfig = applicationConfig.getCanal();
        String zkServers = canalConfig.get("zkServers");
        String destinations = canalConfig.get("destinations");
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destinations, "", "");

        final CanalClient canalClient = new CanalClient(connector, destinations, applicationConfig);
        canalClient.registerRowDataHandler(new KafkaHandlerListener(applicationConfig)); //注册数据处理器
        canalClient.start(); //启动canal
        log.info("server start success......");
    }
}
