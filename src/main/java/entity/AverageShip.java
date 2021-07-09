package entity;

import static utils.Constants.*;
import static utils.Constants.OTHER;

public class AverageShip {

    private Double avg35 = 0.0;
    private Integer ship35 = 0;
    private Double avg6069 = 0.0;
    private Integer ship6069 = 0;
    private Double avg7079 = 0.0;
    private Integer ship7079 = 0;
    private Double avgO = 0.0;
    private Integer shipO = 0;

    public AverageShip(String type, Double avg, int total){
        this.switchType(type, avg, total);
    }

    public void switchType(String type, Double avg, int total){
        switch (type){
            case MILITARY:
                setAvg35(avg);
                setShip35(total);
                break;
            case PASSENGER:
                setAvg6069(avg);
                setShip6069(total);
                break;
            case CARGO:
                setAvg7079(avg);
                setShip7079(total);
                break;
            case OTHER:
                setAvgO(avg);
                setShipO(total);
                break;

        }
    }

    public Double getAvg35() {
        return avg35;
    }

    public Integer getShip35() {
        return ship35;
    }

    public Double getAvg6069() {
        return avg6069;
    }

    public Integer getShip6069() {
        return ship6069;
    }

    public Double getAvg7079() {
        return avg7079;
    }

    public Integer getShip7079() {
        return ship7079;
    }

    public Double getAvgO() {
        return avgO;
    }

    public Integer getShipO() {
        return shipO;
    }

    public void setAvg35(Double avg35) {
        this.avg35 = avg35;
    }

    public void setShip35(Integer ship35) {
        this.ship35 = ship35;
    }

    public void setAvg6069(Double avg6069) {
        this.avg6069 = avg6069;
    }

    public void setShip6069(Integer ship6069) {
        this.ship6069 = ship6069;
    }

    public void setAvg7079(Double avg7079) {
        this.avg7079 = avg7079;
    }

    public void setShip7079(Integer ship7079) {
        this.ship7079 = ship7079;
    }

    public void setAvgO(Double avgO) {
        this.avgO = avgO;
    }

    public void setShipO(Integer shipO) {
        this.shipO = shipO;
    }

    @Override
    public String toString() {
        return "AverageForAnyCell{" +
                "avg35=" + avg35 +
                ", ship35=" + ship35 +
                ", avg6069=" + avg6069 +
                ", ship6069=" + ship6069 +
                ", avg7079=" + avg7079 +
                ", ship7079=" + ship7079 +
                ", avgO=" + avgO +
                ", shipO=" + shipO +
                '}';
    }
}
