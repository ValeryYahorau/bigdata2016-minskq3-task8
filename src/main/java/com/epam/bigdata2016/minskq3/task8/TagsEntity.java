package com.epam.bigdata2016.minskq3.task8;

import java.util.List;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class TagsEntity {

    private int tagsId;
    private List<String> tags;

    public int getTagsId() {
        return tagsId;
    }

    public void setTagsId(int tagsId) {
        this.tagsId = tagsId;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
