package com.leon.canal.handler;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.leon.canal.config.EasyCanalConfig;
import com.leon.canal.handler.base.CommonHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;


@Slf4j
public class EntryHandler implements CommandLineRunner {


    private EasyCanalConfig canalConfig;

    @Autowired
    private Map<String, CommonHandler> handlerMap;


    @Override
    public void run(String... args) throws Exception {
        new Thread((() -> {
            // 创建链接
            CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalConfig.getHost(), canalConfig.getPort()), canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
            log.info("canal 启动成功...");
            int batchSize = 10;
            try {
                connector.connect();
                connector.rollback();
                while (true) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        Thread.sleep(1000);
                        connector.ack(batchId); // 提交确认
                        continue;
                    }

                    handleEntry(message.getEntries());// 数据处理
                    connector.ack(batchId); // 提交确认

                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }

            } catch (Exception e) {
                log.error("canal 异常", e);
            } finally {
                connector.disconnect();
            }
        }), "canal-work").start();

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


            CommonHandler handler = handlerMap.get(dbName + "." + tableName);
            if (handler == null) {
                log.debug("暂不处理, tableName={}", dbName);
                continue;
            }

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                try {
                    if (eventType == CanalEntry.EventType.DELETE) {  // 删除
                        handler.delete(rowData);
                    } else if (eventType == CanalEntry.EventType.INSERT) { // 新增
                        handler.insert(rowData);
                    } else { // 修改
                        handler.update(rowData);
                    }
                } catch (Exception e) {
                    log.error("canal 异常, dbName={}, tableName={}", dbName, tableName, e);
                }

            }
        }
    }

    public EasyCanalConfig getCanalConfig() {
        return canalConfig;
    }

    public void setCanalConfig(EasyCanalConfig canalConfig) {
        this.canalConfig = canalConfig;
    }
}
