package com.fuxg.dto;

/**
 * Created by Administrator on 2016/10/31.
 */
public class KafkaProducerRecord {
    String topic;
    String key;
    String data;

    public KafkaProducerRecord(String topic, String key, String data) {
        this.topic = topic;
        this.key = key;
        this.data = data;
    }

    public static KafkaProducerRecord build(String topic, String key, String data) {
        return new KafkaProducerRecord(topic, key, data);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
