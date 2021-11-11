package com.shujia.function;

import com.shujia.bean.cars;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RealTimeCardWindowProecss extends ProcessWindowFunction<cars, Tuple3<Long,Long,Long>,String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<cars, Tuple3<Long, Long, Long>, String, TimeWindow>.Context context, Iterable<cars> elements, Collector<Tuple3<Long, Long, Long>> out) throws Exception {


        //获取窗口结束时间
        TimeWindow window = context.window();
        long endTime = window.getEnd();

        //统计车流量
        Long sum = 0L;
        for (cars element : elements) {
            sum++;
        }

        //将数据发送到下游
        out.collect(Tuple3.of(Long.parseLong(s), endTime, sum));

    }
}
