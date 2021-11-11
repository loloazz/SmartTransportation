package com.shujia.function;

import com.alibaba.fastjson.JSON;
import com.shujia.bean.cars;
import org.apache.flink.api.common.functions.RichMapFunction;

public class RealTimeCardWindowFlowmapper  extends RichMapFunction<String, cars> {
    @Override
    public cars map(String value) throws Exception {
       return JSON.parseObject(value,cars.class);


    }
}
