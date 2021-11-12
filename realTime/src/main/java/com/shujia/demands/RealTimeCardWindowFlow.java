package com.shujia.demands;

import com.shujia.bean.cars;
import com.shujia.function.RealTImeCardDayFlowMap;
import com.shujia.function.RealTimeCardWindowFlowmapper;
import com.shujia.function.RealTimeCardWindowProecss;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;


/***
 * 未完成
 *
 */
public class RealTimeCardWindowFlow {
    public static void main(String[] args) {


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

        SingleOutputStreamOperator<cars> carsObj = streamSource.map(new RealTimeCardWindowFlowmapper());

        SingleOutputStreamOperator<cars> timestampsAndWatermarks = carsObj
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<cars>forBoundedOutOfOrderness(Duration.ofSeconds(5 * 60))
                                .withTimestampAssigner(new SerializableTimestampAssigner<cars>() {
                                    @Override
                                    public long extractTimestamp(cars element, long recordTimestamp) {

                                        return element.getTime() * 1000L;
                                    }
                                }));


        //取出卡口编号

        SingleOutputStreamOperator<Long> carNum = timestampsAndWatermarks.map(new MapFunction<cars, Long>() {
            @Override
            public Long map(cars value) throws Exception {

                Long car = value.getCard();
                return car;
            }
        });

        /*
         *
         * 1.2 按窗口统计每个卡扣流量 - 统计每个卡扣最近5分钟车流量，每隔1分钟统计一次
         */


        AllWindowedStream<cars, TimeWindow> windowedStream = timestampsAndWatermarks.timeWindowAll(Time.seconds(5), Time.seconds(1));




    }
}
