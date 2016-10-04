package com.epam.bigdata2016.minskq3.task8;

import java.util.Date;
import java.util.List;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class LogEntity {

    private long userTagsId;
    private List<String> tags;
    private String date;
    private int cityId;
    private String city;

    public long getUserTagsId() {
        return userTagsId;
    }

    public void setUserTagsId(long userTagsId) {
        this.userTagsId = userTagsId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "LogEntity{" +
                "userTagsId=" + userTagsId +
                ", tags=" + tags +
                ", date='" + date + '\'' +
                ", cityId=" + cityId +
                ", city='" + city + '\'' +
                '}';
    }
}
