package com.linus.es.demo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yuxuecheng
 * @Title: SimpleStringSearchVO
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/7 15:34
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SimpleStringSearchVO {
    private String indexName;
    private String query;
}
