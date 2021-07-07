package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static utils.KafkaConstants.*;

public class KafkaProperties {

    /**
     * Creates properties for a Kafka Consumer representing the Flink stream source
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getFlinkSourceProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from beginning of partition if no offset was created
       // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100000);
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 2);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    /**
     * Creates properties for a Kafka Producer respresenting the one Flink processing sink
     * @param producerId producer's id
     * @return created properties
     */
    public static Properties getFlinkSinkProperties(String producerId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

        return props;
    }

    /**
     * Creates properties for a Kafka Consumer representing one output subscriber
     * @param consumerGroupId id of consumer group
     * @return created properties
     */
    public static Properties getKafkaParametricConsumerProperties(String consumerGroupId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set consumer group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
        // exactly once semantic
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    /**
     * Creates properties for a Kafka Producer representing the entire stream processing source
     * @param producerId producer's id
     * @return created properties
     */
    public static Properties getKafkaSingleProducerProperties(String producerId) {
        Properties props = new Properties();

        // specify brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // set producer id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        // exactly once semantic
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);

        // key and value serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }
}


