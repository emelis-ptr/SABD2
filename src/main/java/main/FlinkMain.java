package main;

import entity.AutomaticIdentificationSystem;
import entity.ShipMap;
import flink.QueryDue;
import flink.QueryUno;
import kafka.KafkaProperties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import utils.KafkaConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class FlinkMain {

    private static final String CONSUMER_GROUP_ID = "single-flink-consumer";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        streamExecEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //setup flink environment
        //StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // add the source and handle watermarks
        Properties props = KafkaProperties.getFlinkSourceProperties(CONSUMER_GROUP_ID);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaConstants.DATASOURCE_TOPIC, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

        /*DataStream<Tuple2<Long, AutomaticIdentificationSystem>> instanceAIS = AutomaticIdentificationSystem.getInstanceAIS(streamExecEnv, consumer);
        DataStream<ShipMap> instanceMappa = ShipMap.getInstanceMappa(instanceAIS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ShipMap>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((ship, timestamp) -> ship.getTimestamp()));*/

        DataStream<AutomaticIdentificationSystem> instanceAIS = AutomaticIdentificationSystem.getInstanceAIS3(streamExecEnv);
        DataStream<ShipMap> instanceMappa = ShipMap.getInstanceMappa2(instanceAIS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ShipMap>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((ship, timestamp) -> ship.getTimestamp()));

        QueryUno.queryUno(instanceMappa);
        QueryDue.queryDue(instanceMappa);

        streamExecEnv.execute();
    }


}



