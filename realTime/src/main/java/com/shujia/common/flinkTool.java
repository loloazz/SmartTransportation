package com.shujia.common;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * 实现  flink tool工具类
 * 整合创建环境，cheackPiont 以及状态后端
 *
 */
public  class flinkTool {
    private static StreamExecutionEnvironment env = null;


    private flinkTool() throws IOException {

    }


    public  static   StreamExecutionEnvironment getEnviroment() throws IOException {


        if (env==null){
            env = StreamExecutionEnvironment.getExecutionEnvironment();


            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // 每 1000ms 开始一次 checkpoint
            env.enableCheckpointing(10000);

            // 高级选项：

            // 设置模式为精确一次 (这是默认值)
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


            // 确认 checkpoints 之间的时间会进行 500 ms
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

            // Checkpoint 必须在一分钟内完成，否则就会被抛弃
            env.getCheckpointConfig().setCheckpointTimeout(60000);

            // 同一时间只允许一个 checkpoint 进行
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

            // 开启在 job 中止后仍然保留的 externalized checkpoints
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            // 允许在有更近 savepoint 时回退到 checkpoint
            env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

            StateBackend backend = new RocksDBStateBackend("hdfs://hadoop100:9000/car/checkpoint", true);
            env.setStateBackend(backend);

        }

        return env;

    }


}
