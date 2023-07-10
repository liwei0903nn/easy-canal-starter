package com.leon.cdc.flink.entry;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


@Slf4j
public class FastjsonDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {

        JSONObject result = new JSONObject();

        String topic = record.topic();
        String[] source = StringUtils.split(topic, "\\.");
        result.put("tableName", source[1] + "." + source[2]);
        result.put("operation", source);

        Struct value = (Struct) record.value();
        Struct before = value.getStruct("before");
        if (before != null) {
            JSONObject beforeJson = new JSONObject();
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                beforeJson.put(field.name(), before.get(field));
            }
            result.put("before", beforeJson);
        }

        Struct after = value.getStruct("after");
        if (after != null) {
            JSONObject beforeJson = new JSONObject();
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                beforeJson.put(field.name(), after.get(field));
            }
            result.put("after", beforeJson);
        }


        Envelope.Operation operation = Envelope.operationFor(record);
        result.put("operation", operation.toString());

//        log.info("flink cdc binlog 数据, result={}", result);
        out.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
