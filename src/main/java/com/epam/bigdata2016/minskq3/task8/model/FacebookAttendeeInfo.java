package com.epam.bigdata2016.minskq3.task8.model;

import java.io.Serializable;

/**
 * Created by valeryyegorov on 05.10.16.
 */
public class FacebookAttendeeInfo implements Serializable{

    private String name;
    private String id;
    private int count = 0;

    public FacebookAttendeeInfo(String name, String id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
