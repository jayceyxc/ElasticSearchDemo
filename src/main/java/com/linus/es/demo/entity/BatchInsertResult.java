package com.linus.es.demo.entity;

import lombok.Data;

/**
 * @author yuxuecheng
 * @Title: BatchInsertResult
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/12/4 14:56
 */
@Data
public class BatchInsertResult {

    private int successCount = 0;

    private int failedCount = 0;
}
