package com.leon.cdc.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;


@Data
public class FlinkCdcConfig {

    private int sourceParallelism = 1;

    private int sinkParallelism = 1;

    private List<String> databaseList = new ArrayList<>();

    private List<String> tableList = new ArrayList<>();
}
