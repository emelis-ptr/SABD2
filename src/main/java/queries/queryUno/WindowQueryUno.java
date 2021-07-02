package main.java.queries.queryUno;

import main.java.entity.ResultQueryUno;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class WindowQueryUno
        extends ProcessWindowFunction<ResultQueryUno, ResultQueryUno, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<ResultQueryUno> iterable, Collector<ResultQueryUno> collector) {
        ResultQueryUno resultQueryUno = iterable.iterator().next();
        resultQueryUno.setDate(new Date(context.window().getStart()));
        resultQueryUno.setCellId(key);
        collector.collect(resultQueryUno);
    }
}
