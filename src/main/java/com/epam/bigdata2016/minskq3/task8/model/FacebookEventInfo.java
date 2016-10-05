package com.epam.bigdata2016.minskq3.task8.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class FacebookEventInfo implements Serializable{

    private String id;
    private String name;
    private String desc;
    private int attendingCount;
    private String city;
    private String date;
    private String tag;
    private Map<String, Integer> wordsHistogram = new HashMap<String, Integer>();

    public FacebookEventInfo() {
    }

    public FacebookEventInfo(String id, String name, String desc, int attendingCount, String tag) {
        this.id = id;
        this.name = name;
        this.desc = desc;
        this.attendingCount = attendingCount;
        this.tag = tag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getAttendingCount() {
        return attendingCount;
    }

    public void setAttendingCount(int attendingCount) {
        this.attendingCount = attendingCount;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Map<String, Integer> getWordsHistogram() {
        return wordsHistogram;
    }

    public void setWordsHistogram(Map<String, Integer> wordsHistogram) {
        this.wordsHistogram = wordsHistogram;
    }
}
