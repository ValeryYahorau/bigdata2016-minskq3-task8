package com.epam.bigdata2016.minskq3.task8;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class DayCity {

    private String date;
    private String city;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DayCity dayCity = (DayCity) o;

        if (!getDate().equals(dayCity.getDate())) return false;
        return getCity().equals(dayCity.getCity());

    }

    @Override
    public int hashCode() {
        int result = getDate().hashCode();
        result = 31 * result + getCity().hashCode();
        return result;
    }
}
