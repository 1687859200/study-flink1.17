package com.w36.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkWordCountUnbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStreamSource<String> lines = env.socketTextStream("106.15.42.75", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = lines
                .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((Tuple2<String, Integer> stringIntegerTuple2) -> stringIntegerTuple2.f0)
                .sum(1);
        sum.print();

        env.execute();
    }
}
