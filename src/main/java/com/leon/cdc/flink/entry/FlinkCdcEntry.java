package com.leon.cdc.flink.entry;

import com.leon.cdc.config.EasyCdcConfig;
import com.leon.cdc.flink.sink.FlinkDataSinkFunction;
import com.leon.cdc.handler.HandlerManager;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;



@Slf4j
public class FlinkCdcEntry implements ApplicationRunner {

    @Autowired
    private EasyCdcConfig cdcConfig;

    @Autowired
    private HandlerManager handlerManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        new Thread(this::flinkCdcRun, "flink-worker").start();
    }

    public void flinkCdcRun() {
        try {
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname(cdcConfig.getHost())
                    .port(cdcConfig.getPort())
                    .username(cdcConfig.getUsername())
                    .password(cdcConfig.getPassword())
                    .databaseList(cdcConfig.getFlink().getDatabaseList().toArray(new String[0])) // set captured database
                    .tableList(cdcConfig.getFlink().getTableList().toArray(new String[0])) // set captured table
                    .deserializer(new FastjsonDeserializationSchema())
                    .startupOptions(StartupOptions.latest())
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // enable checkpoint
//        env.enableCheckpointing(3000);
//        env.enableCheckpointing(60000 * 60, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///D:/data/flink_checkpoint");
            // 两次 Checkpoint 之间最少间隔 500毫秒
//            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000 * 30);
//            // Checkpoint 过程超时时间为 60000毫秒，即1分钟视为超时失败
//            env.getCheckpointConfig().setCheckpointTimeout(120000);
//            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 同一时间只允许1个Checkpoint的操作在执行

            env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                    .setParallelism(cdcConfig.getFlink().getSourceParallelism())
                    .addSink(new FlinkDataSinkFunction())
                    .setParallelism(cdcConfig.getFlink().getSinkParallelism());

            log.info("flink cdc 正在启动...");
            env.execute();
            log.info("flink cdc 启动成功...");
        } catch (Exception e) {
            log.error("flink cdc 启动失败, config={}", cdcConfig, e);
        }
    }


}
