package kafkaStream;

import entity.AverageQueryUno;
import entity.ShipMap;
import flink.queryUno.AccumulatorQueryUno;
import assigner.WeekTimeWindow;
import kafkaStream.queryUno.AggregatorQueryUnoKafka;
import kafkaStream.queryUno.InitializerQueryUno;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utils.KafkaConstants;
import utils.OutputFormatter;
import utils.serdes.SerDesBuilders;

import java.time.Duration;
import java.time.ZoneId;

import static utils.Constants.ORIENTAL;

/*
      Calcolare per ogni cella del Mar Mediterraneo Occidentale,
      il numero medio di navi militari (SHIPTYPE= 35), navi per trasporto passeggeri (SHIPTYPE = 60-69),
      navi cargo (SHIPTYPE = 70-79) e others(tutte le navi che non hanno uno SHIPTYPE che rientri
      nei casi precedenti) negli ultimi 7 giorni (di event time) e 1 mese (di event time)
     */

public class QueryUnoKafka {

    /**
     * @param instanceShip:
     */
    public static void queryUnoKafka(KStream<Long, ShipMap> instanceShip) {

        //week
        instanceShip
                .map((KeyValueMapper<Long, ShipMap, KeyValue<String, ShipMap>>) (aLong, shipMap)
                        -> new KeyValue<>(shipMap.getCellID(), shipMap))
                .filter((k, v) -> v.getSeaType().equals(ORIENTAL))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(ShipMap.class)))
                //.groupBy((key, value) -> value.getCellID())
                .windowedBy(new WeekTimeWindow(ZoneId.systemDefault(), Duration.ofDays(7)))
                // set up function to aggregate weekly data for average delay
                .aggregate(new InitializerQueryUno(), new AggregatorQueryUnoKafka(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AccumulatorQueryUno.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new MapperQueryUnoKafka())
                // publish results to the correct kafka topic
                .to(KafkaConstants.KAFKA_QUERY_1_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        //month
        instanceShip
                .map((KeyValueMapper<Long, ShipMap, KeyValue<String, ShipMap>>) (aLong, busData)
                        -> new KeyValue<>(busData.getCellID(), busData))
                .filter((k, v) -> v.getSeaType().equals(ORIENTAL))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(ShipMap.class)))
                //.groupBy((key, value) -> value.getCellID())
                .windowedBy(new WeekTimeWindow(ZoneId.systemDefault(), Duration.ofDays(20L)))
                // set up function to aggregate weekly data for average delay
                .aggregate(new InitializerQueryUno(), new AggregatorQueryUnoKafka(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AccumulatorQueryUno.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new MapperQueryUnoKafka())
                // publish results to the correct kafka topic
                .to(KafkaConstants.KAFKA_QUERY_1_MONTHLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }

    /**
     *
     */
    private static class MapperQueryUnoKafka implements KeyValueMapper<Windowed<String>, AccumulatorQueryUno,
            KeyValue<String, String>> {
        @Override
        public KeyValue<String, String> apply(Windowed<String> stringWindowed, AccumulatorQueryUno averageDelayAccumulator) {
            AverageQueryUno outcome = new AverageQueryUno(averageDelayAccumulator.getShipMap());
            return new KeyValue<>(stringWindowed.key(), OutputFormatter.query1OutcomeFormatter(outcome));
        }
    }

}
