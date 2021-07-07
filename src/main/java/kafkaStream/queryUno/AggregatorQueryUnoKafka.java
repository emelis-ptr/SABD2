package kafkaStream.queryUno;

import entity.ShipMap;
import flink.queryUno.AccumulatorQueryUno;
import org.apache.kafka.streams.kstream.Aggregator;

public class AggregatorQueryUnoKafka implements Aggregator<String, ShipMap, AccumulatorQueryUno> {
    @Override
    public AccumulatorQueryUno apply(String s, ShipMap shipMap, AccumulatorQueryUno accumulatorQueryUno) {
        accumulatorQueryUno.add(shipMap.getShipType(), shipMap.getTripId());
        return accumulatorQueryUno;
    }
}
