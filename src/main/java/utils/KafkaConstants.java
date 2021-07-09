package utils;

public class KafkaConstants {

    public static final String DATASOURCE_TOPIC = "datasource-topic";

    public static final String FLINK_QUERY_1_WEEKLY_TOPIC = "flink-query1-weekly";
    public static final String FLINK_QUERY_1_MONTHLY_TOPIC = "flink-query1-monthly";
    public static final String FLINK_QUERY_2_WEEKLY_TOPIC = "flink-query2-weekly";
    public static final String FLINK_QUERY_2_MONTHLY_TOPIC = "flink-query2-monthly";

    public static final String[] FLINK_TOPICS = {FLINK_QUERY_1_WEEKLY_TOPIC,
            FLINK_QUERY_1_MONTHLY_TOPIC, FLINK_QUERY_2_WEEKLY_TOPIC, FLINK_QUERY_2_MONTHLY_TOPIC};

    // if consumer has no offset for the queue starts from the first record
    public static final String CONSUMER_FIRST_OFFSET = "earliest";
    // for exactly once production
    public static final boolean ENABLE_PRODUCER_EXACTLY_ONCE = true;
    public static final String ENABLE_CONSUMER_EXACTLY_ONCE = "read_committed";

    // brokers
    public static final String KAFKA_BROKER_2 = "localhost:9091";
    public static final String KAFKA_BROKER_1 = "kafka1:19091";

    // bootstrap servers
    public static final String BOOTSTRAP_SERVERS =  KAFKA_BROKER_1 + "," + KAFKA_BROKER_2;
}
