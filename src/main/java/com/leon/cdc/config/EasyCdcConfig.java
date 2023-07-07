package com.leon.cdc.config;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author liwei
 * @description
 * @date 2023/7/7 10:15
 */

@ConfigurationProperties(prefix = "easy-cdc")
@Data
public class EasyCdcConfig {

    private String host;

    private Integer port;

    private String username;

    private String password;

    @JSONField(name = "canal")
    private CanalConfig canalConfig;

    @JSONField(name = "flink")
    private FlinkCdcConfig flinkCdcConfig;
}