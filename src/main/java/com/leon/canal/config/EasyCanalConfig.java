package com.leon.canal.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "easy-canal")
@Data
public class EasyCanalConfig {

    private String host;

    private Integer port;

    private String destination;

    private String username;

    private String password;

}
