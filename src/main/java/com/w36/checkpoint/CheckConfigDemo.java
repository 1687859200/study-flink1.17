package com.w36.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class CheckConfigDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // Todo 检查点配置
        // 启用检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("file:///input/chk");
        // 超时时间，默认10分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 同时运行的checkpoint最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 最小等待间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 取消作业时，checkpoint的数据是否保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Todo 非对齐检查点
        // 开启之后自动设为 精准一次，并发为1
        checkpointConfig.enableUnalignedCheckpoints();

        env.socketTextStream("106.15.42.75", 7777)
                .flatMap(
                        (String value, Collector< Tuple2<String,Integer> > out) ->{
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word,1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();


        env.execute();
    }
}
