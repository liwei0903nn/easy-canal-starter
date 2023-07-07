package com.leon.cdc.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.leon.cdc.common.CommonHandler;

import java.util.List;


public abstract class CanalHandler<T> extends CommonHandler<T> {

    public boolean insert(CanalEntry.RowData rowData) {
        T data = convert(rowData.getAfterColumnsList());
        return onInsert(data);
    }


    public boolean update(CanalEntry.RowData rowData) {
        T oldData = convert(rowData.getBeforeColumnsList());
        T newData = convert(rowData.getAfterColumnsList());
        return onUpdate(oldData, newData);
    }


    public boolean delete(CanalEntry.RowData rowData) {
        T data = convert(rowData.getBeforeColumnsList());
        return onDelete(data);
    }


    private T convert(List<CanalEntry.Column> columns) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }

        return jsonObject.toJavaObject(getEClass());
    }

}
