package com.linus.es.demo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: SearchVO
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/6 10:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchVO {
    private String indexName;
    private Map<String, Map<String,Object>> query;
}
