package utils;

import entity.RankQueryDue;
import entity.AverageQueryUno;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Locale;

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
    public static String query1OutcomeFormatter(int days, AverageQueryUno outcome) {
        StringBuilder entryResultBld = new StringBuilder();
        double d;
        entryResultBld.append(outcome.getDate())
                .append(",")
                .append(outcome.getCellId());

        for(int i = 0; i< SHIP_TYPES.length ; i++){
            String type = SHIP_TYPES[i];
            Integer v = outcome.getTypeMap().get(type);
            if(v == null){
                entryResultBld.append(",").append(type).append(",");
            } else {
                d = ((double)v)  / days;
                entryResultBld.append(",").append(type).append(",").append(String.format(Locale.ENGLISH,"%.2f",d));
            }
        }
        return entryResultBld.toString();
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
        int days = Calendar.DAY_OF_WEEK;
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

            builder.append(query1OutcomeFormatter(days, outcome));
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