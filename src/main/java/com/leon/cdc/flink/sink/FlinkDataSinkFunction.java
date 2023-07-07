package com.leon.cdc.flink.sink;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSONObject;
import com.leon.cdc.common.CommonHandler;
import com.leon.cdc.service.HandlerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


@Slf4j
public class FlinkDataSinkFunction extends RichSinkFunction<String> {

    public HandlerService handlerService;

    @Override
    public void invoke(String value, Context context) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        if (handlerService == null) {
            handlerService = SpringUtil.getBean(HandlerService.class);
        }

        String tableName = jsonObject.getString("tableName");
        CommonHandler tableHandler = handlerService.getTableHandler(tableName);
        if (tableHandler == null) {
            log.debug("暂不处理, tableName={}", tableName);
            return;
        }

        Class dataClass = tableHandler.getEClass();
        Object oldData = JSONObject.toJavaObject(jsonObject.getJSONObject("before"), dataClass);
        Object newData = JSONObject.toJavaObject(jsonObject.getJSONObject("after"), dataClass);
        if (oldData != null && newData != null) {
            tableHandler.onUpdate(oldData, newData);
        } else if (newData != null) {
            tableHandler.onInsert(newData);
        } else if (oldData != null) {
            tableHandler.onDelete(oldData);
        }
    }
}
