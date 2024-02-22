package com.w36.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkWordCountStream {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取
        DataStreamSource<String> lines = env.readTextFile("input/word.txt");
        // 处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsTuple = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> of = Tuple2.of(word, 1);
                    collector.collect(of);
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> wordsGroupBy = wordsTuple.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordsGroupBy.sum(1);
        // 输出
        sum.print();
        // 执行
        env.execute();
    }
}
