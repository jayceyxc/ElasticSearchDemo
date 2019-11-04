package com.linus.es.demo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: QueryVO
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:41
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryVO {
    private String indexName;
    private String className;
    private Map<String, Map<String,Object>> query;
}
