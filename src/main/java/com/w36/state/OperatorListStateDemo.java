package com.w36.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("106.15.42.75", 7777)
                .map(new MyCountMapFunction())
                .print();

        env.execute();
    }

    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;
        @Override
        public Long map(String s) throws Exception {
            return ++count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("SnapShotState......");
            state.clear();
            state.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            System.out.println("initializeState......");
            state = functionInitializationContext.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("state", Types.LONG));
            if (functionInitializationContext.isRestored()) {
                for (Long aLong : state.get()) {
                    count += aLong;
                }
            }
        }
    }
}
