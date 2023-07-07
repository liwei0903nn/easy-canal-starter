package com.leon.cdc.service;

import com.leon.cdc.annotations.TableHandler;
import com.leon.cdc.common.CommonHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liwei
 * @description
 * @date 2023/7/7 15:54
 */


@Slf4j
public class HandlerService {

    @Autowired
    private List<CommonHandler> handlerList;

    private Map<String, CommonHandler> tableHanlderMap = new HashMap<>();

    @PostConstruct
    public void initTableMap() {
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
