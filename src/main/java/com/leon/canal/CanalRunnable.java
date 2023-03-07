//package com.leon.canal;
//
//import com.alibaba.otter.canal.client.CanalConnector;
//import com.alibaba.otter.canal.client.CanalConnectors;
//import com.alibaba.otter.canal.protocol.CanalEntry;
//import com.alibaba.otter.canal.protocol.Message;
//import com.leon.canal.annotations.CanalHandler;
//import com.leon.canal.config.EasyCanalConfig;
//import com.leon.canal.handler.base.CommonHandler;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//import java.net.InetSocketAddress;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author liwei
// * @description
// * @date 2023/3/7 15:55
// */
//
//@Slf4j
//@Component
//public class CanalRunnable implements Runnable {
//
//    @Autowired
//    private EasyCanalConfig canalConfig;
//
//    private CanalConnector canalConnector;
//
//
//    @Autowired
//    private List<CommonHandler> handlerList;
//
//    private Map<String, CommonHandler> tableHanlderMap = new HashMap<>();
//
//    @PostConstruct
//    public void initTableMap() {
//        for (CommonHandler commonHandler : handlerList) {
//            CanalHandler canalHandler = commonHandler.getClass().getAnnotation(CanalHandler.class);
//            if (canalHandler == null || canalHandler.tableName().isEmpty()) {
//                continue;
//            }
//
//            tableHanlderMap.put(canalHandler.tableName(), commonHandler);
//        }
//    }
//
//    @Override
//    public void run() {
//        // 创建链接
//        canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getHost(), canalConfig.getPort()), canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
//        log.info("canal 启动成功...");
//        Runtime.getRuntime().addShutdownHook(new Thread(this::disconnect));
//        int batchSize = 10;
//        try {
//            canalConnector.connect();
//            canalConnector.subscribe();
//            canalConnector.rollback();
//            while (true) {
//                Message message = canalConnector.getWithoutAck(batchSize); // 获取指定数量的数据
//                long batchId = message.getId();
//                int size = message.getEntries().size();
//                if (batchId == -1 || size == 0) {
//                    Thread.sleep(1000);
//                    canalConnector.ack(batchId); // 提交确认
//                    continue;
//                }
//
//                handleEntry(message.getEntries());// 数据处理
//                canalConnector.ack(batchId); // 提交确认
//            }
//
//        } catch (Exception e) {
//            log.error("canal 异常, canalConfig={}", canalConfig, e);
//        } finally {
//           disconnect();
//        }
//    }
//
//    private void handleEntry(List<CanalEntry.Entry> entries) {
//        for (CanalEntry.Entry entry : entries) {
//            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
//                continue;
//            }
//
//            CanalEntry.RowChange rowChage = null;
//            try {
//                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
//            } catch (Exception e) {
//                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
//            }
//
//            // 操作类型
//            CanalEntry.EventType eventType = rowChage.getEventType();
//
//            // 数据库
//            String dbName = entry.getHeader().getSchemaName();
//
//            // 表
//            String tableName = entry.getHeader().getTableName();
//
//
//            CommonHandler handler = tableHanlderMap.get(dbName + "." + tableName);
//            if (handler == null) {
//                log.debug("暂不处理, dbName={}, tableName={}", dbName, tableName);
//                continue;
//            }
//
//            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
//                try {
//                    if (eventType == CanalEntry.EventType.DELETE) {  // 删除
//                        handler.delete(rowData);
//                    } else if (eventType == CanalEntry.EventType.INSERT) { // 新增
//                        handler.insert(rowData);
//                    } else if (eventType == CanalEntry.EventType.UPDATE) { // 修改
//                        handler.update(rowData);
//                    }
//                } catch (Exception e) {
//                    log.error("canal 异常, dbName={}, tableName={}", dbName, tableName, e);
//                }
//
//            }
//        }
//    }
//
//    public void disconnect() {
//        if (canalConnector != null) {
//            log.info("canal disconnect...");
//            canalConnector.disconnect();
//        }
//    }
//}
