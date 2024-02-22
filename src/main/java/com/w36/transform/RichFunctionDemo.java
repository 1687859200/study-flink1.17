package com.w36.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        ",子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() + ",调用open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        ",子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() + ",调用close");
            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        });

        map.print();

        env.execute();
    }
}
