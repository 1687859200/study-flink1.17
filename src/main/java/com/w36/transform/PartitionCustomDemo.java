package com.w36.transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> source = env.socketTextStream("106.15.42.75", 7777);

        source.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String s, int i) {
                return Integer.parseInt(s) % i;
            }
        }, r -> r).print();

        env.execute();
    }
}
