package main.java.queries.queryUno;

import main.java.entity.Mappa;
import main.java.entity.ResultQueryUno;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQueryUno implements AggregateFunction<Mappa, AccumulatorQueryUno, ResultQueryUno> {

    @Override
    public AccumulatorQueryUno createAccumulator() {
        return new AccumulatorQueryUno();
    }

    @Override
    public AccumulatorQueryUno add(Mappa data, AccumulatorQueryUno accumulatorQueryUno) {
        accumulatorQueryUno.add(data.getShipType(), data.getTripId());
        return accumulatorQueryUno;
    }

    @Override
    public AccumulatorQueryUno merge(AccumulatorQueryUno acc1, AccumulatorQueryUno acc2) {
        acc2.getTypeMap().forEach(acc1::add);
        return acc1;
    }

    @Override
    public ResultQueryUno getResult(AccumulatorQueryUno accumulator) {
        return new ResultQueryUno(accumulator.getTypeMap());
    }

}
