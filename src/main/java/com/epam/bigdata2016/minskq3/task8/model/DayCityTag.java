package com.epam.bigdata2016.minskq3.task8.model;

import java.io.Serializable;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class DayCityTag implements Serializable {

    private String date;
    private String city;
    private String tag;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DayCityTag that = (DayCityTag) o;

        if (getDate() != null ? !getDate().equals(that.getDate()) : that.getDate() != null) return false;
        if (getCity() != null ? !getCity().equals(that.getCity()) : that.getCity() != null) return false;
        if (getTag() != null ? !getTag().equals(that.getTag()) : that.getTag() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getDate() != null ? getDate().hashCode() : 0;
        result = 31 * result + (getCity() != null ? getCity().hashCode() : 0);
        result = 31 * result + (getTag() != null ? getTag().hashCode() : 0);
        return result;
    }
}