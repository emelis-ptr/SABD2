package main.java.entity;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ResultQueryUno {

    private Date date;
    private String cellId;
    private final Map<String, Integer> typeMap;

    public ResultQueryUno(Map<String, Set<String>> typeMapInput){
        this.typeMap = new HashMap<>();
        for(String shipType: typeMapInput.keySet()){
            this.typeMap.put(shipType, typeMapInput.get(shipType).size());
        }
    }

    public Map<String, Integer> getTypeMap() {
        return typeMap;
    }

    public Date getDate() {
        return date;
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
