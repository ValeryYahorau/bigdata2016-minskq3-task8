package com.epam.bigdata2016.minskq3.task8.model;

import java.util.List;

/**
 * Created by valeryyegorov on 04.10.16.
 */
public class TagsLineEntity {

    private long tagsId;
    private List<String> tags;

    public long getTagsId() {
        return tagsId;
    }

    public void setTagsId(long tagsId) {
        this.tagsId = tagsId;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
