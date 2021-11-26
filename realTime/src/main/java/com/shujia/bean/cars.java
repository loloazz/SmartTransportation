package com.shujia.bean;



public class cars {
    private String car;
    private String city_code;
    private String county_code;
    private Long card;
    private String camera_id;
    private String orientation;
    private Long road_id;
    private Long time;
    private Double speed;


    public cars() {
    }

    public cars(String car, String city_code, String county_code, Long card, String camera_id, String orientation, Long road_id, Long time, Double speed) {

        this.car = car;
        this.city_code = city_code;
        this.county_code = county_code;
        this.card = card;
        this.camera_id = camera_id;
        this.orientation = orientation;
        this.road_id = road_id;
        this.time = time;
        this.speed = speed;
    }

    public String getCar() {
        return car;
    }

    public void setCar(String car) {
        this.car = car;
    }

    public String getCity_code() {
        return city_code;
    }

    public void setCity_code(String city_code) {
        this.city_code = city_code;
    }

    public String getCounty_code() {
        return county_code;
    }

    public void setCounty_code(String county_code) {
        this.county_code = county_code;
    }

    public Long getCard() {
        return card;
    }

    public void setCard(Long card) {
        this.card = card;
    }

    public String getCamera_id() {
        return camera_id;
    }

    public void setCamera_id(String camera_id) {
        this.camera_id = camera_id;
    }

    public String getOrientation() {
        return orientation;
    }

    public void setOrientation(String orientation) {
        this.orientation = orientation;
    }

    public Long getRoad_id() {
        return road_id;
    }

    public void setRoad_id(Long road_id) {
        this.road_id = road_id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }
}
