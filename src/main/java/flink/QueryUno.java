package flink;

import assigner.MonthWindowAssigner;
import entity.ShipMap;
import kafka.KafkaProperties;
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

        double seaOcc = 12.0;

        KeyedStream<ShipMap, String> keyedStream = instanceMappa
                .filter((FilterFunction<ShipMap>) entry -> entry.getLon() < seaOcc)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ShipMap>forBoundedOutOfOrderness(Duration.ofDays(1))
                        .withTimestampAssigner((shipMap, timestamp) -> shipMap.getTimestamp()))
                .keyBy(ShipMap::getCellID);

        keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map(new ResultMapper())
                .addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC,
                                new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC),
                                prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                //.addSink(SinkBuilder.buildSink(RESULTS_DIRECTORY + "/queryUno-week")).setParallelism(1)
                .name("Sink-" + KafkaConstants.FLINK_QUERY_1_WEEKLY_TOPIC);


        keyedStream
                .window(new MonthWindowAssigner())
                .aggregate(new AggregatorQueryUno(), new WindowQueryUno())
                .map(new ResultMapper())
                .addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_1_MONTHLY_TOPIC,
                        new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_1_MONTHLY_TOPIC),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                //.addSink(SinkBuilder.buildSink(RESULTS_DIRECTORY + "/queryUno-Month")).setParallelism(1)
                .name("query1-monthly-flink");
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
