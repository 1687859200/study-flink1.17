package com.w36.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkWordCountBatch {
    public static void main(String[] args) throws Exception {
        // Todo 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Todo 读取数据
        DataSource<String> lines = env.readTextFile("input/word.txt");
        // Todo 切分
        FlatMapOperator<String, Tuple2<String, Integer>> wordsTuple = lines
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    collector.collect(wordTuple2);
                }
            }
        });
        // Todo 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordsGroupBy = wordsTuple.groupBy(0);
        // Todo 聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordsGroupBy.sum(1);
        // Todo 输出
        sum.print();
    }
}
