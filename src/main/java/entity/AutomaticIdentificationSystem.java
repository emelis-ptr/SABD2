package entity;

import java.time.LocalDateTime;

public class AutomaticIdentificationSystem {

    private String shipID; // stringa esadecimale che rappresenta l’identificativo della nave.
    private Integer shipType; // numero intero che rappresenta la tipologia della nave.
    private Integer speed; // numero in virgola mobile che rappresenta la velocità misurata in nodi a cui procede la nave all’istante di segnalazione dell’evento; il separatore decimale è il punto.
    private Double lon; //  numero  in  virgola  mobile  che  rappresenta  la  coordinata  cartesiana  in  gradi  decimali  della longitudine data dal GPS; il separatore decimale è il punto.
    private Double lat; // numero in virgola mobile che rappresenta la coordinata cartesiana in gradi decimali della latitudine data dal sistema GPS; il separatore decimale è il punto.
    private Integer course; // numero intero che rappresenta la direzione del movimento ed è espresso in gradi; è definito come l’angolo in senso orario tra il nord Vero e il punto di destinazione (rotta vera).
    private Integer heading; // numero intero che rappresenta la direzione verso cui la nave è orientata ed è espresso in gradi; è definito come l’angolo in senso orario tra il nord Vero e l’asse longitudinale della barca (pruao prora vera).
    private LocalDateTime timestamp; //  rappresenta  l’istante  temporale  della  segnalazione  dell’evento  AIS;  il  timestamp è espresso con il formato GG-MM-YY hh:mm:ss (giorno, mese, anno, ore, minuti e secondi dell’even-to).
    private String departureportName; // stringa che rappresenta l’identificativo del porto di partenza del viaggioin corso.•
    private Integer reportedDraught; // numero intero che rappresenta la profondità della parte immersa della nave(in centimetri) tra la linea di galleggiamento e la chiglia.
    private String tripID; //stringa alfanumerica che rappresenta l’identificativo del viaggio; è composta dai primi 7 caratteri (inclusi 0x) di SHIPID, concatenati con la data di partenza e di arrivo.

    public AutomaticIdentificationSystem(String shipID, Integer shipType, Integer speed, Double lon, Double lat, Integer course, Integer heading, LocalDateTime timestamp, String departureportName, Integer reportedDraught, String tripID) {
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
}
