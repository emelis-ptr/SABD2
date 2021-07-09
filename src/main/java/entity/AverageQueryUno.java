package entity;

import java.text.SimpleDateFormat;
import java.util.*;

import static utils.Constants.*;

public class AverageQueryUno {

    private Date date;
    private String cellId;
    private final Map<String, Integer> typeMap;

    public AverageQueryUno(Map<String, Set<String>> typeMapInput){
        this.typeMap = new HashMap<>();
        for(String shipType: typeMapInput.keySet()){
            this.typeMap.put(shipType, typeMapInput.get(shipType).size());
        }
    }

    public Map<String, Integer> getTypeMap() {
        return typeMap;
    }

    public String getDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DDMMYYYY);
        return simpleDateFormat.format(date);
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }


    @Override
    public String toString() {
        return "ResultQueryUno{" +
                "date=" + date +
                ", cellId='" + cellId + '\'' +
                ", typeMap=" + typeMap +
                '}';
    }
}
