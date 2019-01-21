package com.neighborhood.aka.laplace.estuary.web.bean;

import java.util.Map;

/**
 * Created by john_liu on 2019/1/21.
 */
public class SdaRequestBean {

    public SdaRequestBean(Map<String,String> tableMappingRule){
        this.tableMappingRule = tableMappingRule;
    }
    private Map<String,String> tableMappingRule;

    public Map<String, String> getTableMappingRule() {
        return tableMappingRule;
    }

    public void setTableMappingRule(Map<String, String> tableMappingRule) {
        this.tableMappingRule = tableMappingRule;
    }
}
