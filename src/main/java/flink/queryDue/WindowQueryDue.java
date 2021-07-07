package flink.queryDue;

import entity.RankQueryDue;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class WindowQueryDue extends ProcessWindowFunction<RankQueryDue, RankQueryDue, String,
        TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<RankQueryDue> iterable,
                        Collector<RankQueryDue> collector) {
        iterable.forEach(k -> {
            k.setDate(new Date(context.window().getStart()));
            k.setSeaType(key);

            collector.collect(k);
        });
    }

}
