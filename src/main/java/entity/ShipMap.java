package entity;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.Utils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import static utils.Constants.*;

public class ShipMap {

    private final String tripId;
    private final String shipID;
    private final String shipType;
    private final String seaType;
    private final String cellID;
    private final double lat;
    private final double lon;
    private final Long timestamp;

    public ShipMap(String tripId, String shipID, int shipType, double lat, double lon, String timestamp) {
        this.tripId = tripId;
        this.shipID = shipID;
        this.shipType = this.defineShipType(shipType);
        this.seaType = this.defineSeaType(lon);
        this.cellID = this.defineCells(lon, lat);
        this.lat = lat;
        this.lon = lon;
        this.timestamp = this.setTimestamp(timestamp);
    }

    private String defineCells(double lon, double lat) {
        char latID = 'A';
        int lonID = 1;
        double dimLat = (MAX_LAT - MIN_LAT) / NUM_CELL_LAT;
        double dimLon = (MAX_LON - MIN_LON) / NUM_CELL_LON;

        int cellaLat = (int) ((lat - MIN_LAT) / dimLat);
        int cellaLon = (int) ((lon - MIN_LON) / dimLon);

        latID += cellaLat;
        lonID += cellaLon;

        return "" + latID + lonID;
    }

    private String defineShipType(int shipType) {
        String type;

        if (shipType == 35) {
            type = MILITARY;
        } else if (shipType >= 60 && shipType <= 69) {
            type = PASSENGER;
        } else if (shipType >= 70 && shipType <= 79) {
            type = CARGO;
        } else {
            type = OTHER;
        }

        return type;
    }

    private String defineSeaType(double longitude) {
        if (longitude <= CANALE_DI_SICILIA_LON) {
            return OCCIDENTAL;
        } else {
            return ORIENTAL;
        }
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

    public String getSeaType() {
        return seaType;
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

    public Long getTimestamp() {
        return timestamp;
    }

    public Long setTimestamp(String time) {
        return Utils.fixTimeStamp(time).getTime();
    }

    public static DataStream<ShipMap> getInstanceMappa(DataStream<Tuple2<Long, AutomaticIdentificationSystem>> dataStream) {

        return dataStream
                .flatMap(new FlatMapFunction<Tuple2<Long, AutomaticIdentificationSystem>, ShipMap>() {
                    @Override
                    public void flatMap(Tuple2<Long, AutomaticIdentificationSystem> entry, Collector<ShipMap> out) {
                        ShipMap shipMap = new ShipMap(
                                entry.f1.getTripID(),
                                entry.f1.getShipID(),
                                Integer.parseInt(entry.f1.getShipType()),
                                entry.f1.getLat(),
                                entry.f1.getLon(),
                                entry.f1.getTimestamp());
                        out.collect(shipMap);
                    }
                })
                .name("mappa");
    }

    public static DataStream<ShipMap> getInstanceMappa2(DataStream<AutomaticIdentificationSystem> dataStream) {

        return dataStream
                .flatMap(new FlatMapFunction<AutomaticIdentificationSystem, ShipMap>() {
                    @Override
                    public void flatMap(AutomaticIdentificationSystem entry, Collector<ShipMap> out) {
                        ShipMap shipMap = new ShipMap(
                                entry.getTripID(),
                                entry.getShipID(),
                                Integer.parseInt(entry.getShipType()),
                                entry.getLat(),
                                entry.getLon(),
                                entry.getTimestamp());
                        out.collect(shipMap);
                    }
                }).name("mappa");
    }

    @Override
    public String toString() {
        return "Mappa{" +
                "tripId='" + tripId + '\'' +
                ", shipID='" + shipID + '\'' +
                ", shipType='" + shipType + '\'' +
                ", seaType='" + seaType + '\'' +
                ", cellID='" + cellID + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", timestamp=" + timestamp +
                '}';
    }

}
