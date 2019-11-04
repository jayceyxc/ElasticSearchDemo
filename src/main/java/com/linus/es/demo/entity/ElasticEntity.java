package com.linus.es.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: ElasticEntity
 * @ProjectName demo
 * @Description: Elastic实体
 * @date 2019-11-02 16:26
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElasticEntity {

    private String id;
    private Map data;
}
