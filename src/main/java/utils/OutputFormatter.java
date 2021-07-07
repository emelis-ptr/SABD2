package utils;

import entity.RankQueryDue;
import entity.AverageQueryUno;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static utils.Constants.*;

/**
 * Class used to format the queries' outcomes into string
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "DuplicatedCode"})
public class OutputFormatter {


    /**
     * Scope: Global
     * Used to remove old files in Results directory
     */
    public static void cleanResultsFolder() {
        try {
            FileUtils.cleanDirectory(new File(RESULTS_DIRECTORY));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not clean Results directory");
        }
    }

    /**
     * Scope: Flink - Query 1
     * Formats the processing outcome as a string to be published to Kafka
     *
     * @param outcome to format
     * @return formatted string
     */
    public static String query1OutcomeFormatter(AverageQueryUno outcome) {
        return outcome.getDate() +
                ";" +
                outcome.getCellId() +
                ";" +
                "Military" +
                ";" +
                outcome.getAvg35() +
                ";" +
                "Passenger" +
                ";" +
                outcome.getAvg6069() +
                ";" +
                "Cargo" +
                ";" +
                outcome.getAvg7079() +
                ";" +
                "Other" +
                ";" +
                outcome.getAvgO();
    }

    /**
     * Scope: Flink - Query 2
     * Formats the processing outcome as a string to be published to Kafka
     *
     * @param outcome to format
     * @return formatted string
     */
    public static String query2OutcomeFormatter(RankQueryDue outcome) {
        return outcome.getDate() +
                ";" +
                outcome.getSeaType() +
                ";" +
                AM +
                ";" +
                outcome.getCellIDInMorning() +
                ";" +
                PM +
                ";" +
                outcome.getCellIDInAfternoon();
    }


    /**
     * @param path    where to store csv
     * @param outcome to be stored
     * @deprecated - needed before Kafka Flink output topic creation
     * Scope: Flink - Query 1
     * Save outcome to csv file and print result
     */
    @Deprecated
    public static void writeOutputQuery1(String path, AverageQueryUno outcome) {
        try {
            // output structures
            File file = new File(path);
            if (!file.exists()) {
                // creates the file if it does not exist
                file.createNewFile();
            }

            // append to existing version of the same file
            FileWriter writer = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(writer);
            StringBuilder builder = new StringBuilder();

            builder.append(query1OutcomeFormatter(outcome));
            builder.append(NEW_LINE);
            bw.append(builder.toString());
            bw.close();
            writer.close();

            // prints formatted output
            if (CONSOLE_RESULTS_PRINT) {
                switch (path) {
                    case QUERY1_WEEKLY_CSV_FILE_PATH:
                        System.out.println(QUERY1_HEADER + WEEKLY_HEADER + builder.toString());
                        break;
                    case QUERY1_MONTHLY_CSV_FILE_PATH:
                        System.out.println(QUERY1_HEADER + MONTHLY_HEADER + builder.toString());
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 1 result to CSV file");
        }
    }

    /**
     * @param path    where to store csv
     * @param outcome to be stored
     * @deprecated - needed before Kafka Flink output topic creation
     * Scope: Flink - Query 2
     * Save outcome to csv file and print result
     */
    @Deprecated
    public static void writeOutputQuery2(String path, RankQueryDue outcome) {
        try {
            // output structures
            File file = new File(path);
            if (!file.exists()) {
                // creates the file if it does not exist
                file.createNewFile();
            }

            // append to existing version of the same file
            FileWriter writer = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(writer);
            StringBuilder builder = new StringBuilder();

            builder.append(query2OutcomeFormatter(outcome));
            builder.append(NEW_LINE);

            bw.append(builder.toString());
            bw.close();
            writer.close();

            // prints formatted output
            if (CONSOLE_RESULTS_PRINT) {
                switch (path) {
                    case QUERY2_MONTHLY_CSV_FILE_PATH:
                        System.out.println(QUERY2_HEADER + MONTHLY_HEADER + builder.toString());
                        break;
                    case QUERY2_WEEKLY_CSV_FILE_PATH:
                        System.out.println(QUERY2_HEADER + WEEKLY_HEADER + builder.toString());
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 2 result to CSV file");
        }
    }


}