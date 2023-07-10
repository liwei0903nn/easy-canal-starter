package com.leon.cdc.flink.sink;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSONObject;
import com.leon.cdc.handler.CommonHandler;
import com.leon.cdc.handler.HandlerManager;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


@Slf4j
public class FlinkDataSinkFunction extends RichSinkFunction<String> {


    @Override
    public void invoke(String value, Context context) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String tableName = jsonObject.getString("tableName");
        HandlerManager handlerManager = SpringUtil.getBean(HandlerManager.class);
        CommonHandler tableHandler = handlerManager.getTableHandler(tableName);
        if (tableHandler == null) {
            log.debug("暂不处理, tableName={}", tableName);
            return;
        }

        Class dataClass = tableHandler.getEClass();
        String operation = jsonObject.getString("operation");
        Object oldData = JSONObject.toJavaObject(jsonObject.getJSONObject("before"), dataClass);
        Object newData = JSONObject.toJavaObject(jsonObject.getJSONObject("after"), dataClass);
        if (Envelope.Operation.UPDATE.toString().equalsIgnoreCase(operation)) {
            tableHandler.onUpdate(oldData, newData);
        } else if (Envelope.Operation.CREATE.toString().equalsIgnoreCase(operation)) {
            tableHandler.onInsert(newData);
        } else if (Envelope.Operation.DELETE.toString().equalsIgnoreCase(operation)) {
            tableHandler.onDelete(oldData);
        }
    }
}
