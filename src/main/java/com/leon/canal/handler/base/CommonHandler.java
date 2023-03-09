package com.leon.canal.handler.base;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;


public abstract class CommonHandler<T> {

    public boolean insert(CanalEntry.RowData rowData) {
        T data = convert(rowData.getAfterColumnsList());
        return onInsert(data);
    }


    public abstract boolean onInsert(T newData);


    public boolean update(CanalEntry.RowData rowData) {
        T oldData = convert(rowData.getBeforeColumnsList());
        T newData = convert(rowData.getAfterColumnsList());
        return onUpdate(oldData, newData);
    }

    public abstract boolean onUpdate(T oldData, T newData);


    public boolean delete(CanalEntry.RowData rowData) {
        T data = convert(rowData.getBeforeColumnsList());
        return onDelete(data);
    }

    public abstract boolean onDelete(T data);

    private Class<T> getEClass() {
        if (tClass != null) {
            return tClass;
        }

        Class<? extends CommonHandler> thisClass = this.getClass();
        Type superClassType = thisClass.getGenericSuperclass();
        ParameterizedType pt = (ParameterizedType) superClassType;
        Type[] genTypeArr = pt.getActualTypeArguments();
        Type genType = genTypeArr[0];
        tClass = (Class<T>) genType;
        return tClass;
    }

    private Class<T> tClass;

    private T convert(List<CanalEntry.Column> columns) {
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : columns) {
            jsonObject.put(column.getName(), column.getValue());
        }

        return jsonObject.toJavaObject(getEClass());
    }

}
