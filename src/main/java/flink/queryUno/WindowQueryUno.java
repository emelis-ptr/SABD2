package flink.queryUno;

import entity.AverageQueryUno;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class WindowQueryUno
        extends ProcessWindowFunction<AverageQueryUno, AverageQueryUno, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<AverageQueryUno> iterable, Collector<AverageQueryUno> collector) {
        iterable.forEach(k -> {

            Date startDate = new Date(context.window().getStart());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            long daysBetween = TimeUnit.DAYS.convert(context.window().getEnd() - context.window().getStart(),
                    TimeUnit.MILLISECONDS);

            k.setDate(new Date(context.window().getStart()));
            k.setCellId(key);

            k.getTypeMap().forEach((k1, v) -> {
                Double avg = (double) v / daysBetween;
                k.switchType(k1, avg, v);
            });

            collector.collect(k);
        });
    }
}
