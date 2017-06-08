package com.fuxg.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fuxg.dto.KafkaProducerRecord;
import com.fuxg.kafka.DataKafkaProducer;
import com.fuxg.util.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by fuxg on 2016/10/26.
 */
public class KafkaHandlerListener implements RowChangeHandlerListener {

    protected final static Logger logger = LoggerFactory.getLogger(KafkaHandlerListener.class);

    DataKafkaProducer kafkaProducer;

    Set<String> subscribeTopics;  //只处理这些表

    public KafkaHandlerListener(ApplicationConfig applicationConfig) {
        kafkaProducer = new DataKafkaProducer(applicationConfig);
    }

    public KafkaHandlerListener(ApplicationConfig applicationConfig, String... subscribeTopics) {
        this.subscribeTopics = new HashSet<String>(Arrays.asList(subscribeTopics));
        kafkaProducer = new DataKafkaProducer(applicationConfig);
    }


    public boolean handler(CanalEntry.Entry entry, CanalEntry.RowChange rowChange) {
        String tableName = entry.getHeader().getTableName().toLowerCase();
        if (subscribeTopics != null && !subscribeTopics.contains(tableName)) {
            return true;
        }
        CanalEntry.EventType eventType = rowChange.getEventType();
        List<CanalEntry.RowData> rows = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rows) {
            send(entry, rowChange, rowData);
        }
        return true;
    }

    /**
     * 把数据发送到kafka
     *
     * @param entry
     * @param rowChange
     * @param rowData
     * @return
     */
    private boolean send(CanalEntry.Entry entry, CanalEntry.RowChange rowChange, CanalEntry.RowData rowData) {
        CanalEntry.EventType eventType = rowChange.getEventType();
        String tableName = entry.getHeader().getTableName().toLowerCase();
        Long timestamp = entry.getHeader().getExecuteTime();

        List<CanalEntry.Column> beforeColumns = rowData.getBeforeColumnsList();
        List<CanalEntry.Column> afterColumns = rowData.getAfterColumnsList();

        String key = "";
        JSONObject before = new JSONObject();
        for (CanalEntry.Column column : beforeColumns) {
            before.put(column.getName(), column.getValue());
            if (column.getIsKey()) {
                key = column.getValue();
            }
        }
        JSONObject after = new JSONObject();
        for (CanalEntry.Column column : afterColumns) {
            after.put(column.getName(), column.getValue());
            if (column.getIsKey()) {
                key = column.getValue();
            }
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("entity", tableName);
        jsonObject.put("before", before);
        jsonObject.put("after", after);
        jsonObject.put("timestamp", timestamp);
        jsonObject.put("type", EVENT_MAPPER.get(eventType));
        String schemal = entry.getHeader().getSchemaName();
        String topic = "synctable_";
        if (!schemal.equals("hsmjia")) {  //兼容老版本
            topic += schemal + "." + tableName;
        } else {
            topic += tableName;
        }
        KafkaProducerRecord kafkaProducerRecord = new KafkaProducerRecord(topic, key, jsonObject.toJSONString());
        kafkaProducer.send(kafkaProducerRecord);
        return true;
    }

    static final Map<CanalEntry.EventType, String> EVENT_MAPPER = new HashMap<CanalEntry.EventType, String>() {
        {
            this.put(CanalEntry.EventType.INSERT, "I");
            this.put(CanalEntry.EventType.UPDATE, "U");
            this.put(CanalEntry.EventType.DELETE, "D");
        }
    };
}
