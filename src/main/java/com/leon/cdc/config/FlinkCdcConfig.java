package com.leon.cdc.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liwei
 * @description
 * @date 2023/7/7 10:32
 */

@Data
public class FlinkCdcConfig {

    private int sourceParallelism = 1;

    private int sinkParallelism = 1;

    private List<String> databaseList = new ArrayList<>();

    private List<String> tableList = new ArrayList<>();
}
