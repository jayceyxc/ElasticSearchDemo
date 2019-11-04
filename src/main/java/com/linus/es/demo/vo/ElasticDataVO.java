package com.linus.es.demo.vo;

import com.linus.es.demo.entity.ElasticEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yuxuecheng
 * @Title: ElasticDataVO
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:39
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ElasticDataVO {
    /**
     * indexName : idx_location
     * elasticEntity : {"id":1,"location":{"location_id":"143831","flag":"Y","local_code":"11","local_name":"市","lv":2,"sup_local_code":"0","url":"11.html"}}
     */

    private String idxName;
    private ElasticEntity elasticEntity;
}
