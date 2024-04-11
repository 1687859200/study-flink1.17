package com.w36.working;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlCDCDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.252.25.5")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.test_dinky_cdc_source")
                .serverTimeZone("GMT+8")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql cdc")
                .print();

        env.execute();
    }
}
