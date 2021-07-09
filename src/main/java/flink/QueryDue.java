package flink;

import assigner.MonthWindowAssigner;
import benchmarks.BenchmarkSink;
import entity.RankQueryDue;
import entity.ShipMap;
import flink.queryDue.AggregatorQueryDue;
import flink.queryDue.WindowQueryDue;
import kafka.KafkaProperties;
import utils.SinkBuilder;
import utils.serdes.FlinkKafkaSerializer;
import utils.KafkaConstants;
import utils.OutputFormatter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import javax.xml.crypto.Data;
import java.time.Duration;
import java.util.Properties;

/*
     Per il Mar Mediterraneo Occidentale ed Orientale fornire la classifica delle tre celle più frequentate
     nelle due fasce orarie di servizio 00:00-11:59 e 12:00-23:59.
     In una determinata fascia oraria, il grado di frequentazione di una cella viene calcolato come il numero
     di navi diverse che attraversano la cellanella fascia oraria in esame
     */
public class QueryDue {

    /**
     *
     * @param instanceMappa:
     */
    public static void queryDue(DataStream<ShipMap> instanceMappa){

        Properties prop = KafkaProperties.getFlinkSinkProperties("producer");

        DataStream<ShipMap> mapDataStream = instanceMappa
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ShipMap>forBoundedOutOfOrderness(Duration.ofDays(1))
                        .withTimestampAssigner((shipMap, timestamp) -> shipMap.getTimestamp()))
                .name("instance-mappa");

        DataStream<String>  streamWeekly = mapDataStream
                .keyBy(ShipMap::getSeaType)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new AggregatorQueryDue(), new WindowQueryDue())
                .map(new ResultMapper())
                .name("flink-query-due-weekly");

        //add sink for producer
        streamWeekly.addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_2_WEEKLY_TOPIC,
                        new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_2_WEEKLY_TOPIC),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name(KafkaConstants.FLINK_QUERY_2_WEEKLY_TOPIC + "-sink");

        //add sink for benchmark
        streamWeekly
                .addSink(new BenchmarkSink())
                .name(KafkaConstants.FLINK_QUERY_2_WEEKLY_TOPIC + "-benchmark");

        streamWeekly.addSink(SinkBuilder.buildSink("results/queryDue-week")).setParallelism(1);

        DataStream<String> streamMonthly = mapDataStream
                .keyBy(ShipMap::getSeaType)
                .window(new MonthWindowAssigner())
                .aggregate(new AggregatorQueryDue(), new WindowQueryDue())
                .map(new ResultMapper())
                .name("flink-query-due-monthly");

        //add sink for producer
        streamMonthly.addSink(new FlinkKafkaProducer<>(KafkaConstants.FLINK_QUERY_2_MONTHLY_TOPIC,
                        new FlinkKafkaSerializer(KafkaConstants.FLINK_QUERY_2_MONTHLY_TOPIC),
                        prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
                .name(KafkaConstants.FLINK_QUERY_2_MONTHLY_TOPIC + "-sink");

        //add sink for benchmark
        streamWeekly
                .addSink(new BenchmarkSink())
                .name(KafkaConstants.FLINK_QUERY_2_MONTHLY_TOPIC + "-benchmark");

        streamMonthly.addSink(SinkBuilder.buildSink("results/queryDue-month")).setParallelism(1);
    }

    /**
     * Mapper
     */
    private static class ResultMapper implements MapFunction<RankQueryDue, String> {
        @Override
        public String map(RankQueryDue outcome)  {
            System.out.println(outcome);
            return OutputFormatter.query2OutcomeFormatter(outcome);
        }
    }


}
