package com.shujia.function;

import com.shujia.bean.cars;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class RealTimeRoadDayAvgSpeedFlowMap extends RichMapFunction<cars, Tuple3<Long,Long,Double>> {
    @Override
    public Tuple3<Long, Long, Double> map(cars value) throws Exception {

        Long road_id = value.getRoad_id();
        Long time = value.getTime()*1000L;
        Double speed = value.getSpeed();

        return  Tuple3.of(road_id,time,speed);


    }
}
