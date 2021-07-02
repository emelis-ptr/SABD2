package main.java.queries.queryUno;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccumulatorQueryUno implements Serializable {

    //mappa (tiponave - set(tripid))
    private final Map<String, Set<String>> typeMap;

    public AccumulatorQueryUno(){
        this.typeMap = new HashMap<>();
    }

    public void add(String shipType, Set<String> tripsSet){
        for (String tripId : tripsSet) {
            add(shipType, tripId);
        }
    }

    public void add(String shipType, String tripId){
        Set<String> typeSet = typeMap.get(shipType);
        //cell found but shipType not found in that cell
        if(typeSet == null){
            typeSet = new HashSet<>();
        }  //update value

        typeSet.add(tripId);
        typeMap.put(shipType, typeSet);
    }

    public Map<String, Set<String>> getTypeMap() {
        return typeMap;
    }

}
