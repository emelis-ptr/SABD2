package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static utils.KafkaConstants.DATASOURCE_TOPIC;

public class ProducerKafka {

    private static final String PRODUCER_ID = "single-producer";
    private final Producer<Long, String> producer;

    public ProducerKafka() {
        producer = createProducer();
    }

    /**
     * Create a new kafka producer
     * @return : the created kafka producer
     */
    private static Producer<Long, String> createProducer() {
        // get the producer properties
        Properties props = KafkaProperties.getKafkaSingleProducerProperties(PRODUCER_ID);
        return new KafkaProducer<>(props);
    }

    /**
     * Function that publish a message to both the flink's and kafka streams' topic
     * @param key: needed to set the key in the kafka streams topic for a correctly process
     * @param value: line to be send
     * @param timestamp: event time
     */
    public void produce(Long key, String value, Long timestamp) {
        final ProducerRecord<Long, String> recordA = new ProducerRecord<>(DATASOURCE_TOPIC, key, value);
        //final ProducerRecord<Long, String> recordB = new ProducerRecord<>(KAFKA_STREAMS_TOPIC, null, timestamp, key, value);

        //send the records
        producer.send(recordA);
        //producer.send(recordB);

        System.out.println("Flink: " + recordA.value());
    }

    /**
     * Flush and close the producer
     */
    public void close() {
        producer.flush();
        producer.close();
    }
}
