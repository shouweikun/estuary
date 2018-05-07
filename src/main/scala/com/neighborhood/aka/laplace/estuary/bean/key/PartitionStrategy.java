package com.neighborhood.aka.laplace.estuary.bean.key;

/**
 * Created by john_liu on 2018/5/6.
 */
public enum PartitionStrategy {
    MOD("MOD"),
    PRIMARY_KEY("PRIMARY_KEY"),
    DATABASE_TABLE("DATABASE_TABLE");



    private String value;
    PartitionStrategy(String value){
        this.value=value;
    }
    public String getValue(){
        return value;
    }
}
