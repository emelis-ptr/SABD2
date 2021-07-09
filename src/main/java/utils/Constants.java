package utils;

public class Constants {

    public static String DIR_CSV = "/data/prj2_dataset.csv";
    private static final String RESULT_PATH = "results/";

    public static final String MILITARY = "MILITARY";
    public static final String PASSENGER = "PASSENGER";
    public static final String CARGO = "CARGO";
    public static final String OTHER = "OTHER";
    public static final String[] SHIP_TYPES = {MILITARY, PASSENGER, CARGO, OTHER};

    public static final String DDMMYYYY = "dd-MM-yyyy";
    public static final double CANALE_DI_SICILIA_LON = 11.797696;
    public static final String OCCIDENTAL = "Occidental";
    public static final String ORIENTAL = "Oriental";

    public static final double MIN_LAT = 32.0;
    public static final double MAX_LAT = 45.0;
    public static final double MIN_LON = -6.0;
    public static final double MAX_LON = 37.0;
    public static final int NUM_CELL_LON = 40;
    public static final int NUM_CELL_LAT = 10;

    public static final boolean CONSOLE_RESULTS_PRINT = false;
    public static final String RESULTS_DIRECTORY = "output";
    public static final String CSV_SEP = ",";
    public static final String NEW_LINE = "\n";
    public static final String AM = "00:00-11:59";
    public static final String PM = "12:00-23:59";
    public static final String WEEKLY_HEADER = "WEEKLY: \t";
    public static final String MONTHLY_HEADER = "MONTHLY: \t";
    public static final String QUERY1_HEADER = "QUERY 1 - ";
    public static final String QUERY2_HEADER = "QUERY 2 - ";

    /**
     * Scope: Flink - Query 1
     */
    public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "output/query1_weekly.csv";
    public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "output/query1_monthly.csv";

    /**
     * Scope: Flink - Query 2
     */
    public static final String QUERY2_MONTHLY_CSV_FILE_PATH = "output/query2_monthly.csv";
    public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "output/query2_weekly.csv";

    public static final String[] FLINK_OUTPUT_FILES = {QUERY1_WEEKLY_CSV_FILE_PATH,
            QUERY1_MONTHLY_CSV_FILE_PATH, QUERY2_MONTHLY_CSV_FILE_PATH, QUERY2_WEEKLY_CSV_FILE_PATH};

}
