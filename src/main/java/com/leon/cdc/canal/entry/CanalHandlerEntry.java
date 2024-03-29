package com.leon.cdc.canal.entry;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.leon.cdc.canal.converter.CanalConverter;
import com.leon.cdc.config.CanalConfig;
import com.leon.cdc.config.EasyCdcConfig;
import com.leon.cdc.handler.CommonHandler;
import com.leon.cdc.handler.HandlerManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.List;


@Slf4j
public class CanalHandlerEntry implements ApplicationRunner {

    private Boolean stop = false;

//    private final Object doneLock = new Object();

    public static final int BATCH_SIZE = 10;

    @Autowired
    private EasyCdcConfig cdcConfig;

    @Autowired
    private HandlerManager handlerManager;


    private CanalConnector canalConnector;


    private Thread consumeThread = null;


    @PreDestroy
    public void destroy() {
//        new Thread(this::stop).start();
        stop();
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        new Thread((() -> {
            do {
                try {
                    initConnector(); // 创建链接
                    consumeData(); // 消费数据
                } catch (Exception e) {
                    log.error("canal 异常, cdcConfig={}", cdcConfig, e);
                } finally {
                    disconnect();
                }

                try {
                    Thread.sleep(10000);  // 等待重连
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } while (!stop);


        }), "canal-work").start();

    }

    private void consumeData() throws Exception {
        if (consumeThread == null) {
            consumeThread = new Thread(() -> {
                while (true) {
                    boolean emptyMsg = false;
                    if (stop) {
                        return;
                    }

                    Message message = canalConnector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
                    emptyMsg = isEmptyMsg(message);
                    if (!emptyMsg) {
                        handleEntry(message.getEntries());// 数据处理
                    }

                    long batchId = message.getId();
                    canalConnector.ack(batchId); // 提交确认

                    if (emptyMsg) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error("consumeData error", e);
                        }
                    }
                }
            }, "canal-consumer");

            consumeThread.start();
        }

        while (!stop) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("consumeData error", e);
            }
        }

    }


    private boolean isEmptyMsg(Message message) {  // 是否空消息
        if (message == null) {
            return true;
        }

        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            return true;
        }

        return false;
    }

    private void initConnector() {
        if (canalConnector == null) {
            CanalConfig canalConfig = cdcConfig.getCanal();
            canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getCanalServerHost(), canalConfig.getCanalServerPort()), canalConfig.getDestination(), canalConfig.getCanalUsername(), canalConfig.getCanalPassword());
        }
        canalConnector.connect();
        canalConnector.subscribe();
        canalConnector.rollback();
        log.info("canal client 启动成功...");
    }

    // 数据处理
    private void handleEntry(List<com.alibaba.otter.canal.protocol.CanalEntry.Entry> entries) {
        for (com.alibaba.otter.canal.protocol.CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == com.alibaba.otter.canal.protocol.CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == com.alibaba.otter.canal.protocol.CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            com.alibaba.otter.canal.protocol.CanalEntry.RowChange rowChage = null;
            try {
                rowChage = com.alibaba.otter.canal.protocol.CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            // 操作类型
            com.alibaba.otter.canal.protocol.CanalEntry.EventType eventType = rowChage.getEventType();

            // 数据库
            String dbName = entry.getHeader().getSchemaName();

            // 表
            String tableName = entry.getHeader().getTableName();


            CommonHandler handler = handlerManager.getTableHandler(dbName + "." + tableName);
            if (handler == null) {
                log.debug("暂不处理, dbName={}, tableName={}", dbName, tableName);
                continue;
            }

            for (com.alibaba.otter.canal.protocol.CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                try {
                    if (eventType == com.alibaba.otter.canal.protocol.CanalEntry.EventType.DELETE) {  // 删除
                        handler.onDelete(CanalConverter.convert(rowData.getBeforeColumnsList(), handler));
                    } else if (eventType == com.alibaba.otter.canal.protocol.CanalEntry.EventType.INSERT) { // 新增
                        handler.onInsert(CanalConverter.convert(rowData.getAfterColumnsList(), handler));
                    } else if (eventType == com.alibaba.otter.canal.protocol.CanalEntry.EventType.UPDATE) { // 修改
                        handler.onUpdate(CanalConverter.convert(rowData.getBeforeColumnsList(), handler),
                                CanalConverter.convert(rowData.getAfterColumnsList(), handler));
                    }
                } catch (Exception e) {
                    log.error("canal client 异常, dbName={}, tableName={}", dbName, tableName, e);
                }

            }
        }
    }

    public void disconnect() {
        if (canalConnector != null) {
            log.info("canal client disconnect...");
            canalConnector.disconnect();
        }
        canalConnector = null;
        consumeThread = null;
    }

    public void stop() {
        log.info("canal client ready stop...");
        stop = true;
        try {
            consumeThread.join(60000);
        } catch (InterruptedException e) {
            log.error("canal client stop fail...", e);
        }
        disconnect();

    }


}
