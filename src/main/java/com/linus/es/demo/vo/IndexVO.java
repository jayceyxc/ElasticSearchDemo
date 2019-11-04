package com.linus.es.demo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: IndexVO
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:40
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexVO {

    /**
     * indexName : idx_location
     * indexSql : {"dynamic":false,"properties":{"location_id":{"type":"long"},"flag":{"type":"text","index":true},"local_code":{"type":"text","index":true},"local_name":{"type":"text","index":true,"analyzer":"ik_max_word"},"lv":{"type":"long"},"sup_local_code":{"type":"text","index":true},"url":{"type":"text","index":true}}}
     */
    private String indexName;
    private IndexSql indexSql;

    public static class IndexSql {
        /**
         * dynamic : false
         * properties : {"location_id":{"type":"long"},"flag":{"type":"text","index":true},"local_code":{"type":"text","index":true},"local_name":{"type":"text","index":true,"analyzer":"ik_max_word"},"lv":{"type":"long"},"sup_local_code":{"type":"text","index":true},"url":{"type":"text","index":true}}
         */

        private boolean dynamic=false;
        private Map<String, Map<String, Object>> properties;

        public boolean isDynamic() {
            return dynamic;
        }

        public void setDynamic(boolean dynamic) {
            this.dynamic = dynamic;
        }

        public Map<String, Map<String, Object>> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Map<String, Object>> properties) {
            this.properties = properties;
        }
    }
}
