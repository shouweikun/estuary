package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/1/22.
 */
public class TableNameMappingBean {

    private String sourceType;                 // 来源类型
    private String sourceTable;      // 老表名
    private String newDb;                 // 新库名
    private String newTable;   // 新表名
    private String sourceDb;

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getNewDb() {
        return newDb;
    }

    public void setNewDb(String newDb) {
        this.newDb = newDb;
    }

    public String getNewTable() {
        return newTable;
    }

    public void setNewTable(String newTable) {
        this.newTable = newTable;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }
}
