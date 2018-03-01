package com.neighborhood.aka.laplce.estuary.bean.support;


import com.neighborhood.aka.laplce.estuary.bean.key.BaseDataJsonKey;

/**
 * Created by z on 17-3-31.
 */
public class KafkaMessage {
        protected BaseDataJsonKey baseDataJsonKey;
        protected String jsonValue;

        public BaseDataJsonKey getBaseDataJsonKey() {
                return baseDataJsonKey;
        }

        public void setBaseDataJsonKey(BaseDataJsonKey baseDataJsonKey) {
                this.baseDataJsonKey = baseDataJsonKey;
        }

        public String getJsonValue() {
                return jsonValue;
        }

        public void setJsonValue(String jsonValue) {
                this.jsonValue = jsonValue;
        }
}
