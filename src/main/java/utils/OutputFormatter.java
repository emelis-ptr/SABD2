package utils;

import entity.AverageShip;
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
     * Formats the processing outcome as a string to be published to Kafka
     *
     * @param outcome to format
     * @return formatted string
     */
    public static String query1OutcomeFormatter(AverageQueryUno outcome) {
        Double avg35 = 0.0;
        Double avg6069 = 0.0;
        Double avg7079 = 0.0;
        Double avgO = 0.0;
        StringBuilder builder = new StringBuilder();

        builder.append(outcome.getDate());
        builder.append(";");
        builder.append(outcome.getCellId());
        builder.append(";");

        for(AverageShip result : outcome.getAverage()) {

            if (result.getShip35() != 0L && result.getAvg35() != 0.0) {
                avg35 = result.getAvg35();
            }
            if (result.getShip6069() != 0L && result.getAvg6069() != 0.0) {
                avg6069 = result.getAvg6069();
            }
            if (result.getShip7079() != 0L && result.getAvg7079() != 0.0) {
                avg7079 = result.getAvg7079();
            }
            if (result.getShipO() != 0L && result.getAvgO() != 0.0) {
                avgO = result.getAvgO();
            }
        }

        builder.append(MILITARY);
        builder.append(";");
        builder.append(avg35);
        builder.append(";");
        builder.append(PASSENGER);
        builder.append(";");
        builder.append(avg6069);
        builder.append(";");
        builder.append(CARGO);
        builder.append(";");
        builder.append(avg7079);
        builder.append(";");
        builder.append(OTHER);
        builder.append(";");
        builder.append(avgO);

        return builder.toString();
    }

    /**
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
     *
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
                        System.out.println(QUERY1_HEADER + WEEKLY_HEADER + builder);
                        break;
                    case QUERY1_MONTHLY_CSV_FILE_PATH:
                        System.out.println(QUERY1_HEADER + MONTHLY_HEADER + builder);
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
     *
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
                        System.out.println(QUERY2_HEADER + MONTHLY_HEADER + builder);
                        break;
                    case QUERY2_WEEKLY_CSV_FILE_PATH:
                        System.out.println(QUERY2_HEADER + WEEKLY_HEADER + builder);
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 2 result to CSV file");
        }
    }


}