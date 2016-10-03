package com.epam.bigdata2016.minskq3.task8;

public class KeyWordAmountEntity {

    private String keyWord;
    private int amount;

    public KeyWordAmountEntity() {
    }

    public KeyWordAmountEntity(String keyWord, int amount) {
        this.keyWord = keyWord;
        this.amount = amount;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}
