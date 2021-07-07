package main;

import entity.AutomaticIdentificationSystem;
import entity.ShipMap;
import kafkaStream.QueryUnoKafka;
import kafkaStream.conf.KafkaStreamConfig;
import utils.KafkaConstants;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamMain {

    public static void main(String[] args) {

        // create kafka streams properties
        Properties props = KafkaStreamConfig.createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        // define input
        KStream<Long, String> inputStream = builder.stream(KafkaConstants.DATASOURCE_TOPIC);
        KStream<Long, AutomaticIdentificationSystem> instanceAIS = AutomaticIdentificationSystem.getInstanceAISKafka(inputStream);
        KStream<Long, ShipMap> instanceShip = ShipMap.getInstanceMappaKafka(instanceAIS);

        QueryUnoKafka.queryUnoKafka(instanceShip);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
