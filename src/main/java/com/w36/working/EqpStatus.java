package com.w36.working;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EqpStatus {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.252.24.5:9092,10.252.24.6:9092,10.252.24.7:9092,10.252.24.8:9092,10.252.24.9:9092")
                .setTopics("data-collection-eap-status")
                .setGroupId("flink-test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        HashMap<String, List<String>> hashMap = new HashMap<>();
        String timeGuard = LocalDate.now().toString() + " 08:00:00";

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("10.252.24.5:9092,10.252.24.6:9092,10.252.24.7:9092,10.252.24.8:9092,10.252.24.9:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("das-collection-eqp-status-sec")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置事务的前缀
                // 事务前缀需跟换，一直使用一个可能存在冲突无法写入
                .setTransactionalIdPrefix("w36-24-4-23-14-")
                // 如果是精准一次 必须设置 事务超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 100 + "")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String eqpID = jsonObject.get("EqpID").toString();
                        String time = jsonObject.get("Puttime").toString();
                        JSONObject data = jsonObject.getJSONObject("Data");
                        String eqpStatus = data.get("EqpStatus").toString();
                        if (hashMap.containsKey(eqpID)) {
                            jsonObject.remove("Data");
                            jsonObject.remove("Puttime");
                            String status = hashMap.get(eqpID).get(0);
                            String start_time = hashMap.get(eqpID).get(1);
                            if (time.compareTo(timeGuard) > 0 && start_time.compareTo(timeGuard) < 0) {
                                ArrayList<ArrayList<String>> arrayLists = dateCut(start_time, time);
                                jsonObject.put("Status", status);
                                for (ArrayList<String> arrayList : arrayLists) {
                                    jsonObject.put("start_time", arrayList.get(0));
                                    jsonObject.put("end_time", arrayList.get(1));
                                    collector.collect(jsonObject.toString());
                                }
                            } else {
                                jsonObject.put("Status", status);
                                jsonObject.put("start_time", start_time);
                                jsonObject.put("end_time", time);
                                collector.collect(jsonObject.toString());
                            }
                        }
                        ArrayList<String> list = new ArrayList<>();
                        list.add(eqpStatus);
                        list.add(time);
                        hashMap.put(eqpID, list);
                    }
                })
                .sinkTo(sink);
//                .print();

        env.execute();
    }

    public static ArrayList<ArrayList<String>> dateCut(String start, String end) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.parse(start, formatter);
        LocalDateTime endTime = LocalDateTime.parse(end, formatter);
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        long days = Duration.between(startTime, endTime).toDays();
        String part = " 08:00:00";
        for (long l = 0; l < days + 2; l++) {
            LocalDateTime plus = startTime.plusDays(l);
            String tmp = plus.toLocalDate().toString() + part;
            if (start.compareTo(tmp) < 0 && end.compareTo(tmp) > 0) {
                ArrayList<String> list = new ArrayList<>();
                list.add(start);
                list.add(tmp);
                start = tmp;
                result.add(list);
            }
        }
        ArrayList<String> list1 = new ArrayList<>();
        list1.add(start);
        list1.add(end);
        result.add(list1);
        return result;
    }
}
