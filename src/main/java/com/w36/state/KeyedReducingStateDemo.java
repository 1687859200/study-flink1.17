package com.w36.state;

import com.w36.bean.WaterSensor;
import com.w36.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("106.15.42.75", 7777)
                .map(new WaterSensorMapFunction());
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);

        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ReducingState<Integer> vcSumReducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcSumReducingState = getRuntimeContext().getReducingState(
                                        new ReducingStateDescriptor<Integer>(
                                                "vcSumReducingState",
                                                new ReduceFunction<Integer>() {
                                                    @Override
                                                    public Integer reduce(Integer integer, Integer t1) throws Exception {
                                                        return integer + t1;
                                                    }
                                                },
                                                Types.INT
                                        )
                                );
                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                vcSumReducingState.add(waterSensor.getVc());
                                Integer vcSum = vcSumReducingState.get();
                                collector.collect("传感器id为" + waterSensor.getId() + ",水位值总和=" + vcSum);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
