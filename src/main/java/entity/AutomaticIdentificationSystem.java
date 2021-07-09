package entity;

import utils.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static utils.Constants.CSV_SEP;

public class AutomaticIdentificationSystem {

    public String shipID; // stringa esadecimale che rappresenta l’identificativo della nave.
    public String shipType; // numero intero che rappresenta la tipologia della nave.
    public String speed; // numero in virgola mobile che rappresenta la velocità misurata in nodi a cui procede la nave all’istante di segnalazione dell’evento; il separatore decimale è il punto.
    public String lon; //  numero  in  virgola  mobile  che  rappresenta  la  coordinata  cartesiana  in  gradi  decimali  della longitudine data dal GPS; il separatore decimale è il punto.
    public String lat; // numero in virgola mobile che rappresenta la coordinata cartesiana in gradi decimali della latitudine data dal sistema GPS; il separatore decimale è il punto.
    public String course; // numero intero che rappresenta la direzione del movimento ed è espresso in gradi; è definito come l’angolo in senso orario tra il nord Vero e il punto di destinazione (rotta vera).
    public String heading; // numero intero che rappresenta la direzione verso cui la nave è orientata ed è espresso in gradi; è definito come l’angolo in senso orario tra il nord Vero e l’asse longitudinale della barca (pruao prora vera).
    public String timestamp; //  rappresenta  l’istante  temporale  della  segnalazione  dell’evento  AIS;  il  timestamp è espresso con il formato GG-MM-YY hh:mm:ss (giorno, mese, anno, ore, minuti e secondi dell’even-to).
    public String departureportName; // stringa che rappresenta l’identificativo del porto di partenza del viaggioin corso.•
    public String reportedDraught; // numero intero che rappresenta la profondità della parte immersa della nave(in centimetri) tra la linea di galleggiamento e la chiglia.
    public String tripID; //stringa alfanumerica che rappresenta l’identificativo del viaggio; è composta dai primi 7 caratteri (inclusi 0x) di SHIPID, concatenati con la data di partenza e di arrivo.

    public AutomaticIdentificationSystem(String shipID, String shipType, String speed, String lon, String lat, String course, String heading, String timestamp, String departureportName, String reportedDraught, String tripID) {
        this.shipID = shipID;
        this.shipType = shipType;
        this.speed = speed;
        this.lon = lon;
        this.lat = lat;
        this.course = course;
        this.heading = heading;
        this.timestamp = timestamp;
        this.departureportName = departureportName;
        this.reportedDraught = reportedDraught;
        this.tripID = tripID;
    }

    public String getShipID() {
        return shipID;
    }

    public String getShipType() {
        return shipType;
    }

    public String getLon() {
        return lon;
    }

    public String getLat() {
        return lat;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getTripID() {
        return tripID;
    }

    /**
     * Flink
     * @param streamExecEnv:
     * @param consumer:
     * @return :
     */
    public static DataStream<Tuple2<Long, AutomaticIdentificationSystem>> getInstanceAIS(StreamExecutionEnvironment streamExecEnv, FlinkKafkaConsumer<String> consumer) {
        return streamExecEnv.addSource(consumer)
                .flatMap(new FlatMapFunction<String, Tuple2<Long, AutomaticIdentificationSystem>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, AutomaticIdentificationSystem>> out) {
                        String[] records = s.split(CSV_SEP);
                        AutomaticIdentificationSystem automaticIdentificationSystem = new AutomaticIdentificationSystem(
                                records[0],
                                records[1],
                                records[2],
                                records[3],
                                records[4],
                                records[5],
                                records[6],
                                records[7],
                                records[8],
                                records[9],
                                records[10]
                        );
                        Long timestamp = Utils.fixTimeStamp(records[7]).getTime();
                        out.collect(new Tuple2<>(timestamp, automaticIdentificationSystem));
                    }
                }).name("source");
    }



    /**
     * Per leggere il file da locale
     *
     * @param streamExecEnv:
     * @return :
     */
    public static DataStream<AutomaticIdentificationSystem> getInstanceAIS3(StreamExecutionEnvironment streamExecEnv) {
        return streamExecEnv
                .readTextFile("file:///C:\\Users\\emeli\\Desktop\\SABD-ProjectTwo\\data\\prj2_dataset.csv")
                .flatMap(new FlatMapFunction<String, AutomaticIdentificationSystem>() {
                    @Override
                    public void flatMap(String s, Collector<AutomaticIdentificationSystem> out) {

                        String[] records = s.split(CSV_SEP);

                        AutomaticIdentificationSystem automaticIdentificationSystem = new AutomaticIdentificationSystem(
                                records[0],
                                records[1],
                                records[2],
                                records[3],
                                records[4],
                                records[5],
                                records[6],
                                records[7],
                                records[8],
                                records[9],
                                records[10]
                        );
                        out.collect(automaticIdentificationSystem);
                    }

                }).name("ais");
    }


    @Override
    public String toString() {
        return "AutomaticIdentificationSystem{" +
                "shipID='" + shipID + '\'' +
                ", shipType=" + shipType +
                ", speed=" + speed +
                ", lon=" + lon +
                ", lat=" + lat +
                ", course=" + course +
                ", heading=" + heading +
                ", timestamp=" + timestamp +
                ", departureportName='" + departureportName + '\'' +
                ", reportedDraught=" + reportedDraught +
                ", tripID='" + tripID + '\'' +
                '}';
    }
}
