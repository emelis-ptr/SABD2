package main.java.queries;

import main.java.entity.AutomaticIdentificationSystem;
import main.java.entity.Mappa;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);;

        DataStream<AutomaticIdentificationSystem> instanceAIS = AutomaticIdentificationSystem.getInstanceAIS3(streamExecEnv);
        DataStream<Mappa> instanceMappa = Mappa.getInstanceMappa(instanceAIS);

        QueryUno.queryUno(instanceMappa);

        streamExecEnv.execute();

    }


}



