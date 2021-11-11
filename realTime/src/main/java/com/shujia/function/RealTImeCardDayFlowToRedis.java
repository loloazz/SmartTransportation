package com.shujia.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.Jedis;

public class RealTImeCardDayFlowToRedis extends RichSinkFunction<Tuple3<Long, String, Long>> {
    private Jedis jedis ;

    @Override
    public void open(Configuration parameters) throws Exception {
         jedis = new Jedis("hadoop100", 6379);


    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }

    @Override
    public void invoke(Tuple3<Long, String, Long> value, Context context) throws Exception {
    String line = "RealTImeCardDayFlow:"+value.f1+":"+value.f0;

    jedis.set(line,value.f2.toString());

    }


}
