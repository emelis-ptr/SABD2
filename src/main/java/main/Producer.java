package main;

import kafka.ProducerKafka;
import utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Producer {

    private static final Long SLEEP = 10L;
    public static final String dirDataset = "data/prj2_dataset.csv";

    @SuppressWarnings({"BusyWait"})
    public static void main(String[] args) {

        // create producer
        ProducerKafka producer = new ProducerKafka();

        String line;
        Long eventTime;

        try {
            // read from file
            FileReader file = new FileReader(Producer.dirDataset);
            BufferedReader bufferedReader = new BufferedReader(file);

            while ((line = bufferedReader.readLine()) != null) {
                try {
                    // produce tuples simulating a data stream processing source
                    String[] info = line.split(",", -1);
                    eventTime = Utils.fixTimeStamp(info[7]).getTime();
                    producer.produce(eventTime, line, eventTime);
                    // for real data stream processing source simulation
                    Thread.sleep(SLEEP);
                } catch (InterruptedException ignored) {
                }
            }

            bufferedReader.close();
            file.close();
            producer.close();
            System.out.println("Producer process completed");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}

