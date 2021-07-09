package flink.queryUno;

import entity.AverageQueryUno;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class WindowQueryUno
        extends ProcessWindowFunction<AverageQueryUno, AverageQueryUno, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<AverageQueryUno> iterable, Collector<AverageQueryUno> collector) {
        iterable.forEach(k -> {

            k.setDate(new Date(context.window().getStart())); //timestamp di inizio della finestra per il risultato
            k.setCellId(key);

            collector.collect(k);
        });
    }
}
