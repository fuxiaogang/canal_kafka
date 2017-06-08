package com.fuxg.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fuxg
 * @create 2017-04-17 11:18
 */
//接收application.yml中的app下面的属性
@Component
@ConfigurationProperties(prefix = "app")
public class ApplicationConfig {
    private Map<String, String> canal = new HashMap<>();
    private Map<String, String> kafka = new HashMap<>();
    private String subscribeTables;

    public Map<String, String> getCanal() {
        return canal;
    }

    public void setCanal(Map<String, String> canal) {
        this.canal = canal;
    }

    public Map<String, String> getKafka() {
        return kafka;
    }

    public void setKafka(Map<String, String> kafka) {
        this.kafka = kafka;
    }

    public String getSubscribeTables() {
        return subscribeTables;
    }

    public void setSubscribeTables(String subscribeTables) {
        this.subscribeTables = subscribeTables;
    }
}
