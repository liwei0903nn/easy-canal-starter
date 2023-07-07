package com.leon.cdc.config;

import com.leon.cdc.canal.entry.CanalEntry;
import com.leon.cdc.flink.entry.FlinkCdcEntry;
import com.leon.cdc.service.HandlerService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({EasyCdcConfig.class})
public class EasyCdcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "easy-cdc", name = "canal")
    public CanalEntry canalEntryHandler() {
        CanalEntry canalEntry = new CanalEntry();
        return canalEntry;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "easy-cdc", name = "flink")
    public FlinkCdcEntry FlinkEntryHandler() {
        FlinkCdcEntry flinkCdcEntry = new FlinkCdcEntry();
        return flinkCdcEntry;
    }

    @Bean
    public HandlerService handlerService() {
        return new HandlerService();
    }


}
