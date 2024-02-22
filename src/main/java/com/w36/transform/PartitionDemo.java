package com.w36.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> source = env.socketTextStream("106.15.42.75", 7777);

        // Todo flink 7种分区器 + 1种自定义
//        source.shuffle().print();
//        source.rebalance().print();
//        source.rescale().print();
        source.broadcast().print();


        env.execute();
    }
}
