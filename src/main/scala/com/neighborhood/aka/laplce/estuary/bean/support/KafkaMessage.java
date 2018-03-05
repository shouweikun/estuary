package com.neighborhood.aka.laplce.estuary.bean.support;


import com.neighborhood.aka.laplce.estuary.bean.key.BaseDataJsonKey;

/**
 * Created by z on 17-3-31.
 */
public class KafkaMessage {
    protected BaseDataJsonKey baseDataJsonKey;
    protected String jsonValue;
    private String journalName;
    private long offset;


    public BaseDataJsonKey getBaseDataJsonKey() {
        return baseDataJsonKey;
    }

    public KafkaMessage() {

    }

    public KafkaMessage(BaseDataJsonKey baseDataJsonKey, String jsonValue) {
        this.baseDataJsonKey = baseDataJsonKey;
        this.jsonValue = jsonValue;
    }

    public KafkaMessage(BaseDataJsonKey baseDataJsonKey, String jsonValue, String journalName, long offset) {
        this.baseDataJsonKey = baseDataJsonKey;
        this.jsonValue = jsonValue;
        this.journalName = journalName;
        this.offset = offset;
    }

    public void setBaseDataJsonKey(BaseDataJsonKey baseDataJsonKey) {
        this.baseDataJsonKey = baseDataJsonKey;
    }

    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getJsonValue() {
        return jsonValue;
    }

    public void setJsonValue(String jsonValue) {
        this.jsonValue = jsonValue;
    }
}
