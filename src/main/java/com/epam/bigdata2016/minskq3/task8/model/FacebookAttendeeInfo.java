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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FacebookAttendeeInfo that = (FacebookAttendeeInfo) o;

        if (getCount() != that.getCount()) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        return !(getId() != null ? !getId().equals(that.getId()) : that.getId() != null);

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        result = 31 * result + getCount();
        return result;
    }
}
