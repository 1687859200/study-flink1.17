package com.w36.process;

import com.w36.bean.WaterSensor;
import com.w36.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("106.15.42.75", 7777)
                .map(new WaterSensorMapFunction());
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> {
                    return element.getTs() * 1000L;
                });
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = sensorDSwithWatermark.keyBy(sensor -> sensor.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                Long currentTime = context.timestamp();// 数据提取的事件时间
                TimerService timerService = context.timerService();     //定时器
//                timerService.registerEventTimeTimer(5000L);  //注册定时器，事件时间=当前watermark
//                System.out.println("当前时间：" + currentTime + "注册5s的定时器");
                long currentTs = timerService.currentProcessingTime();
                timerService.registerProcessingTimeTimer(currentTs + 5000L);
                System.out.println("处理时间"+ currentTs + "注册5秒后的定时器");
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("现在时间：" + timestamp + "定时器触发");
            }
        }).print();


        env.execute();
    }
}

