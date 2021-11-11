package com.shujia.demands;

import com.shujia.bean.cars;

import com.alibaba.fastjson.JSON;
import com.shujia.bean.cars;
import com.shujia.function.RealTImeCardDayFlowMap;
import com.shujia.function.RealTImeCardDayFlowToRedis;
import com.shujia.function.RealTImeRoadDayFlowMap;
import com.shujia.function.RealTimeCardWindowFlowmapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


public class RealTimeRoadDayFlow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        env.setParallelism(1);

        properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");
        properties.setProperty("group.id", "asdasdasdddd");


        FlinkKafkaConsumer<String> cars = new FlinkKafkaConsumer<String>("car", new SimpleStringSchema(), properties);

        cars.setStartFromEarliest();

        DataStreamSource<String> addSource = env.addSource(cars);
        SingleOutputStreamOperator<cars> DSMap = addSource.map(new RealTimeCardWindowFlowmapper());

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> map = DSMap.map(new RealTImeRoadDayFlowMap());


        KeyedStream<Tuple3<Long, String, Long>, Tuple2<Long, String>> keyedStream = map.keyBy(new KeySelector<Tuple3<Long, String, Long>, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> getKey(Tuple3<Long, String, Long> value) throws Exception {

                return Tuple2.of(value.f0, value.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> streamSum = keyedStream.sum(2);

        streamSum.print();


        env.execute();

    }
}
