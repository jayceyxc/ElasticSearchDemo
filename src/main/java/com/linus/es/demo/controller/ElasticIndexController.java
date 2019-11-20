package com.linus.es.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.response.ResponseCode;
import com.linus.es.demo.response.ResponseResult;
import com.linus.es.demo.vo.IndexVO;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: ElasticIndexController
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:33
 */
@Slf4j
@RequestMapping("/elastic/")
@RestController
public class ElasticIndexController {

    @Autowired
    BaseElasticDao baseElasticDao;

    @RequestMapping(value = "/}")
    public ResponseResult index(String index){
        ResponseResult response = new ResponseResult();
        return response;
    }

    @RequestMapping(value = "/createIndex",method = RequestMethod.POST)
    public ResponseResult createIndex(@RequestBody IndexVO idxVo){
        ResponseResult response = new ResponseResult();
        String idxSql = JSONObject.toJSONString(idxVo.getIndexSql());
        log.warn(" indexName={}, indexSql={}",idxVo.getIndexName(),idxSql);
        boolean result = baseElasticDao.createIndex(idxVo.getIndexName(),idxSql);
        if (!result) {
            response.setCode(ResponseCode.FAILED.getCode());
            response.setMsg(ResponseCode.FAILED.getMsg());
        }
        return response;
    }


    @RequestMapping(value = "/exist/{index}")
    public ResponseResult indexExist(@PathVariable(value = "index") String index){
        ResponseResult response = new ResponseResult();
        try {
            if(!baseElasticDao.indexExist(index)) {
                log.error("index={},不存在", index);
                response.setCode(ResponseCode.RESOURCE_NOT_EXIST.getCode());
                response.setMsg(ResponseCode.RESOURCE_NOT_EXIST.getMsg());
            } else {
                log.info("index={}, 已存在", index);
            }
        } catch (ElasticsearchStatusException ese) {

            if (ese.status().getStatus() == 401) {
                log.error(ese.status().name(), ese);
                response.setCode(ese.status().getStatus());
                response.setMsg(ese.getCause().getMessage());
                response.setStatus(false);
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.NETWORK_ERROR.getCode());
            response.setMsg(" 调用ElasticSearch 失败！");
            response.setStatus(false);
        }
        return response;
    }

    @RequestMapping(value = "/del/{index}")
    public ResponseResult indexDel(@PathVariable(value = "index") String index){
        ResponseResult response = new ResponseResult();
        try {
            boolean result = baseElasticDao.deleteIndex(index);
            if (result) {
                response.setMsg("删除索引成功：" + index);
            } else {
                response.setCode(ResponseCode.FAILED.getCode());
                response.setMsg("删除索引失败：" + index);
                response.setStatus(false);
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.NETWORK_ERROR.getCode());
            response.setMsg(" 调用ElasticSearch 失败！");
            response.setStatus(false);
        }
        return response;
    }

    @RequestMapping(value = "/get/{index}")
    public ResponseResult getIndex(@PathVariable(value = "index") String index){
        ResponseResult<Map> response = new ResponseResult<Map>();
        try {
            GetMappingsResponse getMappingsResponse = baseElasticDao.getMappingByIndexName(index);
            Map<String, MappingMetaData> allMappings = getMappingsResponse.mappings();
            MappingMetaData indexMapping = allMappings.get(index);
            Map<String, Object> mapping = indexMapping.getSourceAsMap();
            response.setData(mapping);
        } catch (Exception e) {
            response.setCode(ResponseCode.NETWORK_ERROR.getCode());
            response.setMsg(" 调用ElasticSearch 失败！");
            response.setStatus(false);
        }
        return response;
    }

    @RequestMapping(value = "/alias/{oldIndex}/{aliasIndex}")
    public ResponseResult aliasIndex(@PathVariable(value = "oldIndex") String oldIndex, @PathVariable(value = "aliasIndex") String aliasIndex){
        ResponseResult response = new ResponseResult();
        try {
            boolean result = baseElasticDao.aliasIndex(oldIndex, aliasIndex);
            if (result) {
                response.setMsg("创建索引别名成功：索引名称：" + oldIndex + ", 索引别名：" + aliasIndex);
            } else {
                response.setCode(ResponseCode.FAILED.getCode());
                response.setMsg("创建索引别名失败：索引名称：" + oldIndex + ", 索引别名：" + aliasIndex);
                response.setStatus(false);
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.NETWORK_ERROR.getCode());
            response.setMsg(" 调用ElasticSearch 失败！");
            response.setStatus(false);
        }
        return response;
    }
}
