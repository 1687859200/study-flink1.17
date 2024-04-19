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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EqpStatus {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.252.24.5:9092,10.252.24.6:9092,10.252.24.7:9092,10.252.24.8:9092,10.252.24.9:9092")
                .setTopics("data-collection-eap-status")
                .setGroupId("flink-test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        HashMap<String, List<String>> hashMap = new HashMap<>();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String eqpID = jsonObject.get("EqpID").toString();
                        String time = jsonObject.get("Puttime").toString();
                        JSONObject data = jsonObject.getJSONObject("Data");
                        String eqpStatus = data.get("EqpStatus").toString();
                        if(hashMap.containsKey(eqpID)){
                            jsonObject.remove("Data");
                            jsonObject.remove("Puttime");
                            String status = hashMap.get(eqpID).get(0);
                            String start_time = hashMap.get(eqpID).get(1);
                            jsonObject.put("Status", status);
                            jsonObject.put("start_time", start_time);
                            jsonObject.put("end_time", time);
                            collector.collect(jsonObject.toString());
                        }
                        ArrayList<String> list = new ArrayList<>();
                        list.add(eqpStatus);
                        list.add(time);
                        hashMap.put(eqpID, list);
                    }
                })
                .print();

        env.execute();
    }
}
