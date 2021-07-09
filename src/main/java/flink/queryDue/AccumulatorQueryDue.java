package flink.queryDue;

import java.util.*;

public class AccumulatorQueryDue {

    //cellID - shipID
    private final HashMap<String, Set<String>> am;
    private final HashMap<String, Set<String>> pm;
    //cellID - size set shipID for any cellID
    private final HashMap<String, Integer> frequencyAM;
    private final HashMap<String, Integer> frequencyPM;

    public AccumulatorQueryDue() {
        this.am = new HashMap<>();
        this.pm = new HashMap<>();
        this.frequencyAM = new HashMap<>();
        this.frequencyPM = new HashMap<>();
    }

    public void add(String shipID, long timestamp, String cellID) {

        Calendar calendar = Calendar.getInstance();
        Date date = new Date(timestamp);
        calendar.setTime(date);
        //set  12:00 of the same day
        calendar.set(Calendar.HOUR_OF_DAY, 12);

        //current event date
        Calendar currentCalendar = Calendar.getInstance();
        currentCalendar.setTime(date);

        if (currentCalendar.before(calendar)) {
            addMorning(shipID, cellID);
        } else {
            addAfternoon(shipID, cellID);
        }

    }

    /**
     * Add ship to the morning set
     *
     * @param shipID:
     * @param cellID:
     */
    public void addMorning(String shipID, String cellID) {
        //add to morning
        Set<String> morning = this.am.get(cellID);

        if (morning == null) {
            morning = new HashSet<>();
        }

        morning.add(shipID);

        this.am.put(cellID, morning);
        this.frequencyAM.put(cellID, this.am.get(cellID).size());
    }

    /**
     * Add ship to the afternoon set
     *
     * @param shipID:
     * @param cellID:
     */
    public void addAfternoon(String shipID, String cellID) {
        //add to afternoon
        Set<String> afternoon = this.pm.get(cellID);

        if (afternoon == null) {
            afternoon = new HashSet<>();
        }

        afternoon.add(shipID);

        this.pm.put(cellID, afternoon);
        this.frequencyPM.put(cellID, this.pm.get(cellID).size());
    }

    public void merge(HashMap<String, Set<String>> morningMap, HashMap<String, Set<String>> afternoonMap) {
        morningMap.forEach((k, v) -> v.addAll(this.am.get(k)));
        afternoonMap.forEach((k, v) -> v.addAll(this.pm.get(k)));
    }

    public HashMap<String, Set<String>> getAm() {
        return am;
    }
    public HashMap<String, Set<String>> getPm() {
        return pm;
    }
    public HashMap<String, Integer> getFrequencyAM() {
        return frequencyAM;
    }
    public HashMap<String, Integer> getFrequencyPM() {
        return frequencyPM;
    }
}

