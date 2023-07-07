//package com.leon.cdc.canal.config;
//
//import com.leon.cdc.canal.entry.CanalEntryHandler;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
//import org.springframework.boot.context.properties.EnableConfigurationProperties;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@EnableConfigurationProperties({EasyCanalConfig.class})
//public class EasyCanalAutoConfiguration {
//
//    @Bean
//    @ConditionalOnMissingBean(CanalEntryHandler.class)
//    public CanalEntryHandler entryHandler() {
//        CanalEntryHandler canalEntryHandler = new CanalEntryHandler();
//        return canalEntryHandler;
//    }
//
//}
