package entity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static utils.Constants.DDMMYYYY;

public class RankQueryDue {

    private Date date;
    private String seaType;
    private final ArrayList<String> cellIDInMorning;
    private final ArrayList<String> cellIDInAfternoon;

    public RankQueryDue() {
        this.cellIDInMorning = new ArrayList<>();
        this.cellIDInAfternoon = new ArrayList<>();
    }

    public void setCellIDInMorning(String key) {
        this.cellIDInMorning.add(key);
    }

    public void setCellIDInAfternoon(String key) {
        this.cellIDInAfternoon.add(key);
    }

    public ArrayList<String> getCellIDInMorning() {
        return cellIDInMorning;
    }

    public ArrayList<String> getCellIDInAfternoon() {
        return cellIDInAfternoon;
    }

    public String getDate() {
        SimpleDateFormat sdf = new SimpleDateFormat(DDMMYYYY);
        return sdf.format(this.date);
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getSeaType() {
        return seaType;
    }

    public void setSeaType(String seaType) {
        this.seaType = seaType;
    }

    @Override
    public String toString() {
        return "ResultQueryDue{" +
                "date=" + date +
                ", seaType='" + seaType + '\'' +
                ", cellInMorning=" + cellIDInMorning +
                ", cellInAfternoon=" + cellIDInAfternoon +
                '}';
    }
}
