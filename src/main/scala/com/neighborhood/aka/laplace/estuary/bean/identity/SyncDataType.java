package com.neighborhood.aka.laplace.estuary.bean.identity;

public enum SyncDataType {
    NORMAL("NORMAL"),REMADY("REMADY");
    private String value;
    SyncDataType(String value){
        this.value=value;
    }
    public String getValue(){
        return value;
    }
}
