package com.neighborhood.aka.laplace.estuary.bean.key;

import org.omg.CORBA.TRANSACTION_MODE;

/**
 * Created by john_liu on 2018/5/6.
 */
public enum PartitionStrategy {
    MOD("MOD"),
    PRIMARY_KEY("PRIMARY_KEY"),
    DATABASE_TABLE("DATABASE_TABLE"),
    TRANSACTION("TRANSACTION");  //todo 严格按照事务实现


    private String value;

    PartitionStrategy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static PartitionStrategy fromString(String str) {
        switch (str.toUpperCase()) {
            case "MOD":return PartitionStrategy.MOD;
            case "TRANSACTION":return PartitionStrategy.TRANSACTION;
            case "DATABASE_TABLE":return PartitionStrategy.DATABASE_TABLE;
            case "PRIMARY_KEY":return PartitionStrategy.PRIMARY_KEY;
            default:return PartitionStrategy.PRIMARY_KEY; //默认是primaryKey
        }
    }
}
