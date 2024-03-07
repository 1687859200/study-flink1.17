package com.w36.process;

import com.w36.bean.WaterSensor;
import com.w36.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("106.15.42.75", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyTopNPAWF())
                .print();

        env.execute();
    }

    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow>{
        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
            HashMap<Integer, Integer> vcCountMap = new HashMap<>();
            for (WaterSensor waterSensor : iterable) {
                if(vcCountMap.containsKey(waterSensor.getVc())){
                    vcCountMap.put(waterSensor.getVc(), vcCountMap.get(waterSensor.getVc())+1);
                }else {
                    vcCountMap.put(waterSensor.getVc(), 1);
                }
            }
            List<Tuple2<Integer, Integer>> vcCountList = new ArrayList<>();
            for (Integer key : vcCountMap.keySet()) {
                vcCountList.add(Tuple2.of(key, vcCountMap.get(key)));
            }
            vcCountList.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    return o1.f0 - o2.f0;
                }
            });
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < Math.min(100, vcCountList.size()); i++){
                Tuple2<Integer, Integer> tuple2 = vcCountList.get(i);
                out.append("Top:").append(i+1).append("--").append(tuple2.f0).append(" --> ").append(tuple2.f1).append("\n");
            }
            collector.collect(out.toString());
        }
    }
}
