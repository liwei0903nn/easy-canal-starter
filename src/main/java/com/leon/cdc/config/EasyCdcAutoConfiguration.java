package com.leon.cdc.config;

import com.leon.cdc.canal.entry.CanalHandlerEntry;
import com.leon.cdc.flink.entry.FlinkCdcEntry;
import com.leon.cdc.handler.HandlerManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.leon.cdc.config.EasyCdcConfig.CANAL_TYPE;
import static com.leon.cdc.config.EasyCdcConfig.FLINK_TYPE;

@Configuration
@EnableConfigurationProperties({EasyCdcConfig.class})
@ConditionalOnProperty(prefix = "easy-cdc", name = "enable", havingValue = "true")
public class EasyCdcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "easy-cdc.type", havingValue = CANAL_TYPE)
    public CanalHandlerEntry canalHandlerEntry() {
        CanalHandlerEntry canalHandlerEntry = new CanalHandlerEntry();
        return canalHandlerEntry;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "easy-cdc.type", havingValue = FLINK_TYPE)
    public FlinkCdcEntry flinkEntryHandler() {
        FlinkCdcEntry flinkCdcEntry = new FlinkCdcEntry();
        return flinkCdcEntry;
    }

    @Bean
    public HandlerManager handlerManager() {
        return new HandlerManager();
    }


}
