package com.w36.working;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.w36.bean.EqpSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;

public class KafkaHandle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HashMap<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition("data-collection-eap-catch-small-boat", 0), 601180L);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.252.24.5:9092,10.252.24.6:9092,10.252.24.7:9092,10.252.24.8:9092,10.252.24.9:9092")
                .setGroupId("data-collection-eap-catch-small-boat-flink")
                .setTopics("data-collection-eap-catch-small-boat")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.offsets(specificOffsets))
                .build();

        SingleOutputStreamOperator<EqpSensor> flatMap = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .flatMap(new FlatMapFunction<String, EqpSensor>() {
                    @Override
                    public void flatMap(String s, Collector<EqpSensor> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String eqpLetter = jsonObject.get("EqpLetter").toString();
                        String eqpID = jsonObject.get("EqpID").toString();
                        String eqpBlk = jsonObject.get("EqpBlk").toString();
                        String eqpModel = jsonObject.get("EqpModel").toString();
                        String puttime = jsonObject.get("Puttime").toString();
                        JSONObject data = jsonObject.getJSONObject("Data");
                        for (String key : data.keySet()) {
                            EqpSensor eqpSensor = new EqpSensor();
                            eqpSensor.setEqpBlk(eqpBlk);
                            eqpSensor.setEqpId(eqpID);
                            eqpSensor.setEqpLetter(eqpLetter);
                            eqpSensor.setEqpModel(eqpModel);
                            eqpSensor.setPuttime(puttime);
                            eqpSensor.setCol(key);
                            eqpSensor.setValue(data.get(key).toString());
                            collector.collect(eqpSensor);
                        }
                    }
                });
//        flatMap.print();
        flatMap.addSink(JdbcSink.sink(
                "insert into ads_flink_catch_small_boat_t values(?,?,?,?,?,?,?)",
                (ps, eqpSensor) -> {
                    ps.setString(1, eqpSensor.getEqpLetter());
                    ps.setString(2, eqpSensor.getPuttime());
                    ps.setString(3, eqpSensor.getEqpId());
                    ps.setString(4, eqpSensor.getEqpBlk());
                    ps.setString(5, eqpSensor.getEqpModel());
                    ps.setString(6, eqpSensor.getCol());
                    ps.setString(7, eqpSensor.getValue());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(10)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://10.252.21.17:9030/ads")
                        .withUsername("root")
                        .withPassword("z4mit@aiko")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        ));

        env.execute();
    }
}
