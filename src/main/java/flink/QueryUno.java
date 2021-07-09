package flink;

import assigner.MonthWindowAssigner;
import benchmarks.BenchmarkSink;
import entity.ShipMap;
import kafka.KafkaProperties;
import utils.SinkBuilder;
import utils.serdes.FlinkKafkaSerializer;
import utils.KafkaConstants;
import utils.OutputFormatter;
import flink.queryUno.AggregatorQueryUno;
import entity.AverageQueryUno;
import flink.queryUno.WindowQueryUno;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

import static utils.Constants.ORIENTAL;

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
    public static void queryUno(DataStream<ShipMap> instanceMappa) {

        Properties prop = KafkaProperties.getFlinkSinkProperties("producer");

        DataStream<ShipMap> shipMapDataStream = instanceMappa
                .filter((FilterFunction<ShipMap>) entry -> entry.getSeaType().equals(ORIENTAL))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ShipMap>forBoundedOutOfOrderness(Duration.ofDays(1))
                        .withTimestampAssigner((shipMap, timestamp) -> shipMap.getTimestamp()))
                .name("filtered-query-uno");

        DataStream<String> streamWeekly = shipMapDataStream
                .keyBy(ShipMap::getCellID)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map(new ResultMapper())
                .name("flink-query-one-weekly");

        //add sink for producer
        streamWeekly
                .addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC,
                                new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("Sink-" + KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC);

        //add sink for benchmark
        streamWeekly
                .addSink(new BenchmarkSink())
                .name(KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC + "-benchmark");

        streamWeekly.addSink(SinkBuilder.buildSink("results/queryUno-week")).setParallelism(1);

        DataStream<String> streamMonthly = shipMapDataStream
                .keyBy(ShipMap::getCellID)
                .window(new MonthWindowAssigner())
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map(new ResultMapper())
                .name("flink-query-one-monthly");

        //add sink for producer
        streamMonthly.addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_1_MONTHLY_TOPIC,
                        new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_1_MONTHLY_TOPIC),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name("query1-monthly-flink");

        streamMonthly.addSink(SinkBuilder.buildSink("results/queryUno-Month")).setParallelism(1);

        //add sink for benchmark
        streamMonthly
                .addSink(new BenchmarkSink())
                .name(KafkaConstants.FLINK_QUERY_1_MONTHLY_TOPIC + "-benchmark");

    }


    /**
     *
     */
    private static class ResultMapper implements MapFunction<AverageQueryUno, String> {
        @Override
        public String map(AverageQueryUno outcome) {
            System.out.println(outcome);
            return OutputFormatter.query1OutcomeFormatter(outcome);
        }
    }
}
