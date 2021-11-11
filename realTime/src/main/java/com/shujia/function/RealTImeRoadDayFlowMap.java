package com.shujia.function;

import com.shujia.bean.cars;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;

public class RealTImeRoadDayFlowMap  extends RichMapFunction<cars, Tuple3<Long, String, Long>>  {
    private SimpleDateFormat dateFormat = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        dateFormat = new SimpleDateFormat("yyyyMMdd");

    }

    @Override
    public Tuple3<Long, String, Long> map(cars s) throws Exception {

        Long Road_id = s.getRoad_id();

        String day = dateFormat.format(s.getTime()*1000L);


        return  Tuple3.of(Road_id,day,1l);

    }
}
