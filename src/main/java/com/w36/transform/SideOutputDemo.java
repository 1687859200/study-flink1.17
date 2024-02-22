package com.w36.transform;

import com.w36.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("tag1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tage = new OutputTag<>("tag2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                // 侧输出流
                if ("s1".equals(id)) {
                    ctx.output(s1Tag, value);
                // 侧输出流
                } else if ("s2".equals(id)) {
                    ctx.output(s2Tage, value);
                } else {
                    out.collect(value);
                }
            }
        });
        // 打印主流数据
        process.print();
        // 打印s1
//        process.getSideOutput(s1Tag).print();
        // 打印s2
        process.getSideOutput(s2Tage).printToErr();

        env.execute();
    }
}
