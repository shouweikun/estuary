package com.neighborhood.aka.laplace.estuary.web.bean;

import java.util.Map;
import java.util.Set;

/**
 * Created by john_liu on 2019/1/21.
 */
public class SdaRequestBean {

    public SdaRequestBean(Map<String,String> tableMappingRule){
        this.tableMappingRule = tableMappingRule;
    }

    public SdaRequestBean(Map<String, String> tableMappingRule, Map<String, Set<String>> encryptField) {
        this.tableMappingRule = tableMappingRule;
        this.encryptField = encryptField;
    }

    private Map<String,String> tableMappingRule;

    private Map<String,Set<String>> encryptField;

    public Map<String, Set<String>> getEncryptField() {
        return encryptField;
    }

    public void setEncryptField(Map<String, Set<String>> encryptField) {
        this.encryptField = encryptField;
    }

    public Map<String, String> getTableMappingRule() {
        return tableMappingRule;
    }

    public void setTableMappingRule(Map<String, String> tableMappingRule) {
        this.tableMappingRule = tableMappingRule;
    }
}
