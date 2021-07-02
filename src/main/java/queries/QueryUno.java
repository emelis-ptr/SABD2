package main.java.queries;

import main.java.assigner.MonthWindowAssigner;
import main.java.assigner.WeekWindowAssigner;
import main.java.entity.Mappa;
import main.java.utils.SinkBuilder;
import main.java.queries.queryUno.AggregatorQueryUno;
import main.java.entity.ResultQueryUno;
import main.java.queries.queryUno.WindowQueryUno;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

    /*
      Calcolare per ogni cella del Mar Mediterraneo Occidentale,
      il numero medio di navi militari (SHIPTYPE= 35), navi per trasporto passeggeri (SHIPTYPE = 60-69),
      navi cargo (SHIPTYPE = 70-79) e others(tutte le navi che non hanno uno SHIPTYPE che rientri
      nei casi precedenti) negli ultimi 7 giorni (di event time) e 1 mese (di event time)
     */

public class QueryUno {

    /**
     * @param instanceMappa:
     */
    public static void queryUno(DataStream<Mappa> instanceMappa) {

        double getSeaOcc = 12.0;

        KeyedStream<Mappa, String> windowedStream = instanceMappa
                .filter((FilterFunction<Mappa>) entry -> entry.getLon() < getSeaOcc)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Mappa>forBoundedOutOfOrderness(Duration.ofDays(1))
                        .withTimestampAssigner((mappa, timestamp) -> mappa.getTimestamp().getTime()))
                .keyBy(Mappa::getCellID);

        windowedStream
                .window(new WeekWindowAssigner())
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map((MapFunction<ResultQueryUno, String>) query1Outcome -> {
                    int windowWeek = Calendar.DAY_OF_WEEK;
                    return queryResultAppend(windowWeek, query1Outcome);
                })
                .addSink(SinkBuilder.buildSink("results/" + "queryUno-week")).setParallelism(1);

        windowedStream
                .window(new MonthWindowAssigner())
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map((MapFunction<ResultQueryUno, String>) resultQueryUno -> {
                    int windowWeek = Calendar.DAY_OF_MONTH;
                    return queryResultAppend(windowWeek, resultQueryUno);
                })
                .addSink(SinkBuilder.buildSink("results/" + "queryUno-Month")).setParallelism(1);

        windowedStream.print();
    }

    /**
     *
     * @param windowEventTime:
     * @param resultQueryUno:
     * @return :
     */
    private static String queryResultAppend(int windowEventTime, ResultQueryUno resultQueryUno) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        StringBuilder builder = new StringBuilder();
        Date date = resultQueryUno.getDate();
        builder.append(simpleDateFormat.format(date))
                .append(",").append(resultQueryUno.getCellId());

        resultQueryUno.getTypeMap().forEach((k, v) -> {
            builder.append(",").append(k).append(",").append(String.format(Locale.ENGLISH, "%.2g", (double) v / windowEventTime));
        });

        return builder.toString();
    }
}
