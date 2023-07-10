package com.leon.cdc.handler;

import com.leon.cdc.annotations.TableHandler;
import com.leon.cdc.handler.CommonHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



@Slf4j
public class HandlerManager {

    @Autowired(required = false)
    private List<CommonHandler> handlerList;

    private Map<String, CommonHandler> tableHanlderMap = new HashMap<>();

    @PostConstruct
    public void initTableMap() {
        if (CollectionUtils.isEmpty(handlerList)){
            return;
        }

        for (CommonHandler commonHandler : handlerList) {
            TableHandler tableHandler = commonHandler.getClass().getAnnotation(TableHandler.class);
            if (tableHandler == null || tableHandler.tableName().isEmpty()) {
                continue;
            }

            tableHanlderMap.put(tableHandler.tableName(), commonHandler);
        }
    }

    public CommonHandler getTableHandler(String tableName) {
        return tableHanlderMap.get(tableName);
    }


}
