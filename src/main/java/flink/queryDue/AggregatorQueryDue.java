package flink.queryDue;

import entity.RankQueryDue;
import entity.ShipMap;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;
import java.util.stream.Collectors;

public class AggregatorQueryDue implements AggregateFunction<ShipMap, AccumulatorQueryDue, RankQueryDue> {

    @Override
    public AccumulatorQueryDue createAccumulator() {
        return new AccumulatorQueryDue();
    }

    @Override
    public AccumulatorQueryDue add(ShipMap shipMap, AccumulatorQueryDue accumulatorQueryDue) {
        accumulatorQueryDue.add(shipMap.getShipID(), shipMap.getTimestamp(), shipMap.getCellID());
        return accumulatorQueryDue;
    }

    @Override
    public RankQueryDue getResult(AccumulatorQueryDue accumulatorQueryDue) {
        //Create the lists from elements of HashMap
        List<Map.Entry<String, Integer>> am = new LinkedList<>(accumulatorQueryDue.getFrequencyAM().entrySet());
        List<Map.Entry<String, Integer>> pm = new LinkedList<>(accumulatorQueryDue.getFrequencyPM().entrySet());

        //Sort the lists in descending order
        am.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        pm.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        RankQueryDue rankQueryDue = new RankQueryDue();

        rankQueryDue.setCellIDInMorning(am.stream().map(Map.Entry::getKey).limit(3).collect(Collectors.toList()).toString());
        rankQueryDue.setCellIDInAfternoon(pm.stream().map(Map.Entry::getKey).limit(3).collect(Collectors.toList()).toString());

        return rankQueryDue;
    }

    @Override
    public AccumulatorQueryDue merge(AccumulatorQueryDue acc1, AccumulatorQueryDue acc2) {
        acc1.merge(acc2.getAm(), acc2.getPm());
        return acc1;
    }
}
