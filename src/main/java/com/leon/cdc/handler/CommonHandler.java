package com.leon.cdc.handler;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


public abstract class CommonHandler<T> {


    public abstract boolean onInsert(T newData);


    public abstract boolean onUpdate(T oldData, T newData);


    public abstract boolean onDelete(T data);

    public Class<T> getEClass() {
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

}
