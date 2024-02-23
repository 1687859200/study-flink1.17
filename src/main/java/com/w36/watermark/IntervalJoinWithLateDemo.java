package com.w36.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .socketTextStream("106.15.42.75", 7777)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return Tuple2.of(datas[0], Integer.valueOf(datas[1]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );


        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .socketTextStream("106.15.42.75", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return Tuple3.of(datas[0], Integer.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r -> r.f0);
        OutputTag<Tuple2<String, Integer>> ks1Late = new OutputTag<>("ks1Late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2Late = new OutputTag<>("ks2Late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(ks1Late)
                .sideOutputRightLateData(ks2Late)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + " <----> " + right);
                    }
                });

        process.print();
        process.getSideOutput(ks1Late).printToErr();
        process.getSideOutput(ks2Late).printToErr();

        env.execute();
        env.execute();
    }
}
