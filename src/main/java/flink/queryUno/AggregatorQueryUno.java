package flink.queryUno;

import entity.AverageQueryUno;
import entity.ShipMap;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQueryUno implements AggregateFunction<ShipMap, AccumulatorQueryUno, AverageQueryUno> {

    @Override
    public AccumulatorQueryUno createAccumulator() {
        return new AccumulatorQueryUno();
    }

    @Override
    public AccumulatorQueryUno add(ShipMap data, AccumulatorQueryUno accumulatorQueryUno) {
        accumulatorQueryUno.add(data.getShipType(), data.getTripId());
        return accumulatorQueryUno;
    }

    @Override
    public AccumulatorQueryUno merge(AccumulatorQueryUno acc1, AccumulatorQueryUno acc2) {
        acc2.getShipMap().forEach(acc1::add);
        return acc1;
    }

    @Override
    public AverageQueryUno getResult(AccumulatorQueryUno accumulator) {
        return new AverageQueryUno(accumulator.getShipMap());
    }

}
