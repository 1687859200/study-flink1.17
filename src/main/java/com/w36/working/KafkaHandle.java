package com.w36.working;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;

public class KafkaHandle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HashMap<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition("data-collection-eap-catch-small-boat", 0), 594880L);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.252.24.5:9092,10.252.24.6:9092,10.252.24.7:9092,10.252.24.8:9092,10.252.24.9:9092")
                .setGroupId("data-collection-eap-catch-small-boat-flink")
                .setTopics("data-collection-eap-catch-small-boat")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.offsets(specificOffsets))
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String eqpID = jsonObject.get("EqpID").toString();
                        JSONObject data = jsonObject.getJSONObject("Data");
                        for (String key : data.keySet()) {
                            JSONObject object = new JSONObject();
                            object.put("EqpID", eqpID);
                            object.put("col", key);
                            object.put("value", data.get(key));
                            collector.collect(object.toString());
                        }
                    }
                })
                .print();

        env.execute();
    }
}
