package com.epam.bigdata2016.minskq3.task8.model;

import java.util.List;

public class TagEvents {

    private String tag;
    private List<FacebookEventInfo> events;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public List<FacebookEventInfo> getEvents() {
        return events;
    }

    public void setEvents(List<FacebookEventInfo> events) {
        this.events = events;
    }
}
