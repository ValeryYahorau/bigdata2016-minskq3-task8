package com.epam.bigdata2016.minskq3.task8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultEntity {

    private String keyWord;
    private int totalAmountAttendees;
    private List<String> eventIds;
    private Map<String, Long> tokenMap;

    public ResultEntity() {
        this.eventIds = new ArrayList<>();
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public int getTotalAmountAttendees() {
        return totalAmountAttendees;
    }

    public void setTotalAmountAttendees(int totalAmountAttendees) {
        this.totalAmountAttendees = totalAmountAttendees;
    }

    public List<String> getEventIds() {
        return eventIds;
    }

    public void setEventIds(List<String> eventIds) {
        this.eventIds = eventIds;
    }

    public Map<String, Long> getTokenMap() {
        return tokenMap;
    }

    public void setTokenMap(Map<String, Long> tokenMap) {
        this.tokenMap = tokenMap;
    }
}
