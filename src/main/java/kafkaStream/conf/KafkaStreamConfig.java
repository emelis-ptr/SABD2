package kafkaStream.conf;

import utils.KafkaConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaStreamConfig {

    /**
     * Create properties used in kafka streams
     * @return :
     */
    public static Properties createStreamProperties() {

        Properties props = new Properties();

        // application id
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-queries");
        // client id
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-streams-queries-client");
        // list of brokers
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        // increase commit interval to avoid window intermediate result flush
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 60000);

        // key and value serdes
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return props;
    }
}
