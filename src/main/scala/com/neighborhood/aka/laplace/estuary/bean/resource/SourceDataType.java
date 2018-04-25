package com.neighborhood.aka.laplace.estuary.bean.resource;

public enum SourceDataType {
    MYSQL("MYSQL"),MONGO("MONGO");
    private String value;
    SourceDataType(String value){
        this.value=value;
    }
    public String getValue(){
        return value;
    }
}
