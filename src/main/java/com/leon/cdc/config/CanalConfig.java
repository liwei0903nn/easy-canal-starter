package com.leon.cdc.config;

import lombok.Data;


@Data
public class CanalConfig {
    private String canalServerHost = "localhost";
    private int canalServerPort = 11111;
    private String destination = "example";
    private String canalUsername;
    private String canalPassword;
}
