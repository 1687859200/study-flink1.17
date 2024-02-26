package com.w36.process;

import com.w36.bean.WaterSensor;
import com.w36.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;


public class KeyedProcessTopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("106.15.42.75", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorDS.keyBy(sensor -> sensor.getVc())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new vcCountAgg(), new WindowResult());

        windowAgg.keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();

        env.execute();
    }

    public static class vcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        @Override
        public void process(Integer integer, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
            Integer count = iterable.iterator().next();
            long windowEnd = context.window().getEnd();
            collector.collect(Tuple3.of(integer, count, windowEnd));
        }
    }
    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>{
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context context, Collector<String> collector) throws Exception {
            Long windowEnd = value.f2;
            if(dataListMap.containsKey(windowEnd)){
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            }else{
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataListMap.put(windowEnd, dataList);
            }
            context.timerService().registerEventTimeTimer(windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            StringBuilder output = new StringBuilder();
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++){
                Tuple3<Integer, Integer, Long> tuple3 = dataList.get(i);
                output.append(tuple3.f2).append(" Top:").append(i+1).append("--").append(tuple3.f0).append(" --> ").append(tuple3.f1).append("\n");
            }
            dataList.clear();
            out.collect(output.toString());
        }
    }
}
