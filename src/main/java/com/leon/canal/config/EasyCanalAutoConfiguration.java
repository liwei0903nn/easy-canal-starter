package com.leon.canal.config;

import com.leon.canal.handler.EntryHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({EasyCanalConfig.class})
public class EasyCanalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EntryHandler.class)
    public EntryHandler entryHandler() {
        EntryHandler entryHandler = new EntryHandler();
        return entryHandler;
    }

}
