package flink.queryUno;

import entity.AverageShip;
import entity.AverageQueryUno;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WindowQueryUno
        extends ProcessWindowFunction<AverageQueryUno, AverageQueryUno, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<AverageQueryUno> iterable, Collector<AverageQueryUno> collector) {
        iterable.forEach(k -> {

            List<AverageShip> averageShips = new ArrayList<>();
            Date startDate = new Date(context.window().getStart());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            long daysBetween = TimeUnit.DAYS.convert(context.window().getEnd() - context.window().getStart(),
                    TimeUnit.MILLISECONDS);

            k.setDate(new Date(context.window().getStart()));
            k.setCellId(key);

            k.getTypeMap().forEach((k1, v) -> {
                Double avg = (double) v / daysBetween;
                AverageShip averageShip = new AverageShip(k1, avg, v);
                averageShips.add(averageShip);
            });

            k.setAverage(averageShips);
            collector.collect(k);
        });
    }
}
