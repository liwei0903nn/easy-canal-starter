package com.leon.cdc.canal.converter;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.leon.cdc.handler.CommonHandler;

import java.util.List;


public class CanalConverter {


    public static <T> T convert(List<CanalEntry.Column> columns, CommonHandler<T> commonHandler) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }

        return jsonObject.toJavaObject(commonHandler.getEClass());
    }

}
