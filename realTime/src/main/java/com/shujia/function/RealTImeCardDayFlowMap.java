package com.shujia.function;

import com.alibaba.fastjson.JSON;
import com.shujia.bean.cars;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;


public class RealTImeCardDayFlowMap extends  RichMapFunction<String, Tuple3<Long,String,Long>>{
  private   SimpleDateFormat dateFormat = null;

    @Override
    public void open(Configuration parameters) throws Exception {
         dateFormat = new SimpleDateFormat("yyyyMMdd");


    }

    @Override
    public Tuple3<Long, String, Long> map(String s) throws Exception {
        cars cars = JSON.parseObject(s, cars.class);

        Long card = cars.getCard();

        String day = dateFormat.format(cars.getTime()*1000L);


        return  Tuple3.of(card,day,1l);

    }
}

