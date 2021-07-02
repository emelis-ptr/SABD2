package main.java.entity;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import utils.Utils;

import java.util.Date;

public class Mappa {

    private final String tripId;
    private final String shipID;
    private final String shipType;
    private final String cellID;
    private final double lat;
    private final double lon;
    private final Date timestamp;

    public Mappa(String tripId, String shipID, int shipType, double lat, double lon, String timestamp) {
        this.tripId = tripId;
        this.shipID = shipID;
        this.shipType = this.setShipType(shipType);
        this.cellID = this.setCells(lon, lat);
        this.lat = lat;
        this.lon = lon;
        this.timestamp = this.setTimestamp(timestamp);
    }

    private String setCells(double lon, double lat){
        double minLat = 32.0;
        double maxLat = 45.0;
        double minLon = -6.0;
        double maxLon = 37.0;

        double dimLat = (maxLat - minLat)/10;
        double dimLon = (maxLon - minLon)/40;

        char cellaLat = (char)('A' + ((int) Math.ceil((lat - minLat)/dimLat)) -1);
        int cellaLon = (int) Math.ceil((lon - minLon)/dimLon);

        return "" + cellaLat + cellaLon;
    }

    private String setShipType(int shipType){
        String type = null;

        if(shipType == 35){
            type = "MILITARY";
        } else if(shipType >= 60 && shipType <=69 ) {
            type = "PASSENGERS";
        } else if (shipType >= 70 && shipType <=79) {
            type = "CARGO";
        } else {
            type = "OTHERS";
        }

        return  type;
    }

    public String getTripId() {
        return tripId;
    }

    public String getShipID() {
        return shipID;
    }

    public String getShipType() {
        return shipType;
    }

    public String getCellID() {
        return cellID;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Date setTimestamp(String time){
        return Utils.fixTimeStamp(time);
    }

    public static DataStream<Mappa> getInstanceMappa(DataStream<AutomaticIdentificationSystem> dataStream){

        return dataStream
                .flatMap(new FlatMapFunction<AutomaticIdentificationSystem, Mappa>() {
                    @Override
                    public void flatMap(AutomaticIdentificationSystem entry, Collector<Mappa> out) throws Exception {
                        Mappa mappa = new Mappa(
                                entry.getTripID(),
                                entry.getShipID(),
                                Integer.parseInt(entry.getShipType()),
                                Double.parseDouble(entry.getLat()),
                                Double.parseDouble(entry.getLon()),
                                entry.getTimestamp());
                        out.collect(mappa);
                    }
                }).name("mappa");
    }

    @Override
    public String toString() {
        return "Mappa{" +
                "tripId='" + tripId + '\'' +
                ", shipID='" + shipID + '\'' +
                ", shipType='" + shipType + '\'' +
                ", cellID='" + cellID + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", timestamp=" + timestamp +
                '}';
    }
}
