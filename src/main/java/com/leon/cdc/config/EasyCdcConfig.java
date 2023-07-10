package com.leon.cdc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "easy-cdc")
@Data
public class EasyCdcConfig {

    public static final String FLINK_TYPE = "flink";
    public static final String CANAL_TYPE = "canal";

    private boolean enable = true;

    private String type = FLINK_TYPE;

    private boolean logRawData = false;

    private String host;

    private Integer port;

    private String username;

    private String password;


    private CanalConfig canal;

    private FlinkCdcConfig flink;
}
