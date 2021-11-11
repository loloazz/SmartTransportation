package com.shujia.demands;

import com.shujia.function.RealTImeCardDayFlowToRedis;
import com.shujia.function.RealTImeCardDayFlowMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

/**
 * 需求：实时统计每个卡扣当天总流量
 *
 * 思路：通过解析kafka数据   对卡口号和日期求和
 */

public class RealTImeCardDayFlow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //读取卡口过车数据

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");
        properties.setProperty("group.id", "asdsadsa");

        //创建消费者

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "car",
                new SimpleStringSchema(),
                properties);


        DataStreamSource<String> streamSource = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> streamOperator = streamSource.map(new RealTImeCardDayFlowMap());


        KeyedStream<Tuple3<Long, String, Long>, Tuple2<Long, String>> keyedStream = streamOperator.keyBy(new KeySelector<Tuple3<Long, String, Long>, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> getKey(Tuple3<Long, String, Long> value) throws Exception {


                return Tuple2.of(value.f0, value.f1);
            }
        });


        //统计车流量
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> sum = keyedStream.sum(2);
        sum.print();


        /**
         * 将数据保存到redis
         */
        DataStreamSink<Tuple3<Long, String, Long>> streamSink = sum.addSink(new RealTImeCardDayFlowToRedis());

        env.execute();


    }

}
