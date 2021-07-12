package utils;

import entity.RankQueryDue;
import entity.AverageQueryUno;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import static utils.Constants.*;

/**
 * Class used to format the queries' outcomes into string
 */

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
        StringBuilder stringBuilder = new StringBuilder();
        double d;
        stringBuilder.append(outcome.getDate())
                .append(",")
                .append(outcome.getCellId());

        for (String type : SHIP_TYPES) {
            Integer v = outcome.getTypeMap().get(type);
            if (v == null) {
                stringBuilder.append(",").append(type).append(",");
            } else {
                d = ((double) v) / days;
                stringBuilder.append(",").append(type).append(",").append(String.format(Locale.ENGLISH, "%.2f", d));
            }
        }
        return stringBuilder.toString();
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

}