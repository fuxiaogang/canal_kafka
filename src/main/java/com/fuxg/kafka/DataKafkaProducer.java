package com.fuxg.kafka;

import com.fuxg.dto.KafkaProducerRecord;
import com.fuxg.util.ApplicationConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Administrator on 2016/10/27.
 */
public class DataKafkaProducer {

    private KafkaProducer producer = null;
    private BlockingQueue<KafkaProducerRecord> messageQueue;
    static final int queueSize = 5000;
    private Thread worker;
    private boolean running = true;
    private ApplicationConfig applicationConfig;

    public DataKafkaProducer(ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        createProducer();
        messageQueue = new LinkedBlockingQueue<>(queueSize);
        worker = new Thread(() -> {
            while (running) {
                try {
                    KafkaProducerRecord message = messageQueue.take();
                    ProducerRecord<String, Object> data = new ProducerRecord<String, Object>(message.getTopic(), message.getKey(), message.getData());
                    Future<RecordMetadata> future = producer.send(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        worker.start();
    }

    private void createProducer() {
        Map<String, String > kafkaconfig = applicationConfig.getKafka();
        Properties properties = new Properties();
        properties.put("bootstrap.servers",kafkaconfig.get("bootstrapServers"));
        properties.put("key.serializer",kafkaconfig.get("keySerializer"));
        properties.put("value.serializer",kafkaconfig.get("valueSerializer"));
        properties.put("acks",kafkaconfig.get("acks"));
        producer = new KafkaProducer<String, String>(properties);
    }

    public void send(KafkaProducerRecord message) {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        while (!messageQueue.isEmpty()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        running = false;
        if (producer != null)
            producer.close();
    }
}
