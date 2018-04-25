package com.neighborhood.aka.laplace.estuary.bean.datasink;

public enum SinkDataType {
    KAFKA("KAFKA");
    private String value;
    SinkDataType(String value){
        this.value=value;
    }
    public String getValue(){
        return value;
    }
}
