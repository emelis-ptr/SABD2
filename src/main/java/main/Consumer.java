package main;

import kafka.ConsumerKafka;
import utils.OutputFormatter;

import java.util.ArrayList;
import java.util.Scanner;

import static utils.Constants.FLINK_OUTPUT_FILES;
import static utils.KafkaConstants.FLINK_TOPICS;

/**
 * Class used to launch consumers for Flink and Kafka Streams output
 */
public class Consumer {

    public static void main(String[] args) {

        //cleaning result directory to store data results
        OutputFormatter.cleanResultsFolder();
        System.out.println("Result directory prepared");

        // create a consumer structure to allow stopping
        ArrayList<ConsumerKafka> consumers = new ArrayList<>();

        int id = 0;
        // launch Flink topics consumers
        for (int i = 0; i < FLINK_TOPICS.length; i++) {
            ConsumerKafka consumer = new ConsumerKafka(id,
                    FLINK_TOPICS[i],
                    FLINK_OUTPUT_FILES[i]);
            consumers.add(consumer);
            new Thread(consumer).start();
            id++;
        }

        System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
        Scanner scanner = new Scanner(System.in);
        // wait for the user to digit something
        scanner.next();
        System.out.println("Sending shutdown signal to consumers");
        // stop consumers
        for (ConsumerKafka consumer : consumers) {
            consumer.stop();
        }
        System.out.flush();
        System.out.println("Signal sent, closing...");
    }
}