package com.shujia.demands;

import com.shujia.bean.cars;
import com.shujia.function.RealTimeCardWindowFlowmapper;
import com.shujia.function.RealTimeRoadDayAvgSpeedFlowMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.Period;
import java.util.Iterator;
import java.util.Properties;

/**
 * 实时统计五分钟的平均车速
 * 一分钟计算一次
 */
public class RealTimeRoadDayAvgSpeedFlow {

    public static void main(String[] args) throws Exception {
//"car":"皖AG94D3","city_code":"340100","county_code":"340102","card":117314031873010,"camera_id":"00103","orientation":"西南","road_id":34400884,"time":1614732331,"speed":37.99


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取卡口过车数据

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");
        properties.setProperty("group.id", "asdsadsa");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("car", new SimpleStringSchema(), properties);


        DataStreamSource<String> source = env.addSource(kafkaConsumer);


        SingleOutputStreamOperator<cars> carObj = source.map(new RealTimeCardWindowFlowmapper());

        /**
         *  设置最大误差时间
         */
        SingleOutputStreamOperator<cars> assignTimestampsAndWatermarks = carObj.assignTimestampsAndWatermarks(WatermarkStrategy.<cars>forBoundedOutOfOrderness(Duration.ofMinutes(1)).withTimestampAssigner(new SerializableTimestampAssigner<cars>() {
            @Override
            public long extractTimestamp(cars element, long recordTimestamp) {
                return element.getTime() * 1000L;

            }
        }));


        KeyedStream<cars, Long> keyedStream = assignTimestampsAndWatermarks.keyBy(new KeySelector<cars, Long>() {
            @Override
            public Long getKey(cars value) throws Exception {
                return value.getRoad_id();
            }
        });

        // 设置窗口类型
        AllWindowedStream<cars, TimeWindow> windowedStream = keyedStream.timeWindowAll(Time.seconds(5), Time.seconds(1));


        SingleOutputStreamOperator<Tuple3<Long, Long, Double>> processDS = windowedStream.process(new ProcessAllWindowFunction<cars, Tuple3<Long, Long, Double>, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<cars, Tuple3<Long, Long, Double>, TimeWindow>.Context context, Iterable<cars> elements, Collector<Tuple3<Long, Long, Double>> out) throws Exception {
                long num = 0L;
                double sumSpeed = 0.0;
                Iterator<cars> iterator = elements.iterator();
                cars cars = iterator.next();

                while (iterator.hasNext()) {
                    cars car = iterator.next();
                    Double speed = car.getSpeed();
                    sumSpeed += speed;
                    num++;
                }


                double avgSpeed = sumSpeed / num;

                long endTime = context.window().getEnd();

                out.collect(Tuple3.of(cars.getRoad_id(), endTime, avgSpeed));
            }
        });

        processDS.print();


        env.execute("RealTimeRoadDayAvgSpeedFlow");


    }
}
