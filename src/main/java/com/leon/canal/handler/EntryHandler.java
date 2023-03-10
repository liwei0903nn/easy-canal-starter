package com.leon.canal.handler;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.leon.canal.annotations.CanalHandler;
import com.leon.canal.config.EasyCanalConfig;
import com.leon.canal.handler.base.CommonHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class EntryHandler implements ApplicationRunner {

    private Boolean stop = false;

    private final Object doneLock = new Object();

    public static final int BATCH_SIZE = 10;

    @Autowired
    private EasyCanalConfig canalConfig;

    @Autowired
    private List<CommonHandler> handlerList;


    private CanalConnector canalConnector;

    private Map<String, CommonHandler> tableHanlderMap = new HashMap<>();

    @PostConstruct
    public void initTableMap() {
        for (CommonHandler commonHandler : handlerList) {
            CanalHandler canalHandler = commonHandler.getClass().getAnnotation(CanalHandler.class);
            if (canalHandler == null || canalHandler.tableName().isEmpty()) {
                continue;
            }

            tableHanlderMap.put(canalHandler.tableName(), commonHandler);
        }

//        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    @PreDestroy
    public void destroy() {
        new Thread(this::stop).start();
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        new Thread((() -> {
            do {
                try {
                    initConnector(); // 创建链接
                    consumeData(); // 消费数据
                } catch (Exception e) {
                    log.error("canal 异常, canalConfig={}", canalConfig, e);
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

    private void consumeData() throws InterruptedException {
        while (true) {
            boolean emptyMsg = false;
            synchronized (doneLock) {
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
            }

            if (emptyMsg) {
                Thread.sleep(1000);
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
            canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getHost(), canalConfig.getPort()), canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
        }
        canalConnector.connect();
        canalConnector.subscribe();
        canalConnector.rollback();
        log.info("canal client 启动成功...");
    }

    // 数据处理
    private void handleEntry(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            // 操作类型
            CanalEntry.EventType eventType = rowChage.getEventType();

            // 数据库
            String dbName = entry.getHeader().getSchemaName();

            // 表
            String tableName = entry.getHeader().getTableName();


            CommonHandler handler = tableHanlderMap.get(dbName + "." + tableName);
            if (handler == null) {
                log.debug("暂不处理, dbName={}, tableName={}", dbName, tableName);
                continue;
            }

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                try {
                    if (eventType == CanalEntry.EventType.DELETE) {  // 删除
                        handler.delete(rowData);
                    } else if (eventType == CanalEntry.EventType.INSERT) { // 新增
                        handler.insert(rowData);
                    } else if (eventType == CanalEntry.EventType.UPDATE) { // 修改
                        handler.update(rowData);
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
    }

    public void stop() {
        log.info("canal client ready stop...");
        synchronized (doneLock) {
            log.info("canal client stop...");
            stop = true;
            disconnect();
        }

    }


}
