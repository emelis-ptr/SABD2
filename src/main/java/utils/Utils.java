package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    /**
     * Metodo che converte i diversi pattern in un unico pattern uguale
     *
     * @param tokens:
     * @return :
     */
    public static Date fixTimeStamp(String tokens) {
        SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd/MM/yy HH:mm");
        String[] split = tokens.split("[/\\-\\:\\ ]");

        String day = split[0];
        String month = split[1];
        String year = split[2];
        String hours = split[3];

        if (split[0].length() == 1) {
            day = 0 + "" + split[0];
        }

        if (split[1].length() == 1) {
            month = 0 + "" + split[1];
        }

        if (split[2].length() == 1) {
            year = 0 + "" + split[2];
        }

        if (split[3].length() == 1) {
            hours = 0 + "" + split[3];
        }

        String dateStamp = day + "/" + month + "/" + year + " " + hours + ":" + split[4];

        Date localDate = null;
        try {
            localDate = dateTimeFormatter.parse(dateStamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return localDate;
    }
}
