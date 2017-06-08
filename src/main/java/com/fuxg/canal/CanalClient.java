package com.fuxg.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fuxg.handler.RowChangeHandlerListener;
import com.fuxg.util.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by fuxg on 2016/10/26.
 */
public class CanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(CanalClient.class);
    private ApplicationConfig applicationConfig;
    protected CanalConnector connector;
    protected String destination;
    final int batchSize = 1000;
    protected volatile boolean running = false;

    protected List<RowChangeHandlerListener> rowChangeHandlerListeners = new ArrayList<>(); //数据处理器

    protected Thread thread = null;

    protected static String row_format = "->binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms";

    public CanalClient() {
    }

    public CanalClient(CanalConnector connector, String destination, ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        this.connector = connector;
        this.destination = destination;
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(() -> {
            process();
        });
        thread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("parse events has an error", e);
        });
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void process() {
        try {
            connector.connect();
            connector.subscribe(applicationConfig.getSubscribeTables());
            while (running) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {  //没有变化的数据
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                } else {
                    rowdataHandle(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } catch (Exception e) {
            logger.error("process error!", e);
        } finally {
            connector.disconnect();
        }
    }

    protected void rowdataHandle(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    logger.error("parse events has an error, data:" + entry.toString(), e);
                    return;
                }

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), rowChange.getEventType(),
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});

                //获取变化的数据，给具体的业务处理器处理
                for (RowChangeHandlerListener h : rowChangeHandlerListeners) {
                    h.handler(entry, rowChange);
                }
            }
        }
    }

    public void registerRowDataHandler(RowChangeHandlerListener rowChangeHandlerListener) {
        rowChangeHandlerListeners.add(rowChangeHandlerListener);
    }
}
