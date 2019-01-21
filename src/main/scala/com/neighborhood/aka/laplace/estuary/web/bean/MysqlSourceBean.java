package com.neighborhood.aka.laplace.estuary.web.bean;



import java.util.List;

/**
 * Created by john_liu on 2019/1/17.
 */
public class MysqlSourceBean {

    private MysqlCredentialRequestBean master;
    private List<String>  concernedDatabase;
    private List<String>  ignoredDatabase;
    private String  filterPattern;
    private String filterBlackPattern;
    private boolean filterQueryDcl;
    private boolean filterQueryDml;
    private boolean filterQueryDdl;
    private boolean filterRows;
    private boolean filterTableError;

    public MysqlCredentialRequestBean getMaster() {
        return master;
    }

    public void setMaster(MysqlCredentialRequestBean master) {
        this.master = master;
    }

    public List<String> getConcernedDatabase() {
        return concernedDatabase;
    }

    public void setConcernedDatabase(List<String> concernedDatabase) {
        this.concernedDatabase = concernedDatabase;
    }

    public List<String> getIgnoredDatabase() {
        return ignoredDatabase;
    }

    public void setIgnoredDatabase(List<String> ignoredDatabase) {
        this.ignoredDatabase = ignoredDatabase;
    }

    public String getFilterPattern() {
        return filterPattern;
    }

    public void setFilterPattern(String filterPattern) {
        this.filterPattern = filterPattern;
    }

    public String getFilterBlackPattern() {
        return filterBlackPattern;
    }

    public void setFilterBlackPattern(String filterBlackPattern) {
        this.filterBlackPattern = filterBlackPattern;
    }

    public boolean isFilterQueryDcl() {
        return filterQueryDcl;
    }

    public void setFilterQueryDcl(boolean filterQueryDcl) {
        this.filterQueryDcl = filterQueryDcl;
    }

    public boolean isFilterQueryDml() {
        return filterQueryDml;
    }

    public void setFilterQueryDml(boolean filterQueryDml) {
        this.filterQueryDml = filterQueryDml;
    }

    public boolean isFilterQueryDdl() {
        return filterQueryDdl;
    }

    public void setFilterQueryDdl(boolean filterQueryDdl) {
        this.filterQueryDdl = filterQueryDdl;
    }

    public boolean isFilterRows() {
        return filterRows;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

    public boolean isFilterTableError() {
        return filterTableError;
    }

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }
}
