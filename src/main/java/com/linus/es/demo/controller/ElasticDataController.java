package com.linus.es.demo.controller;

import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.entity.ElasticEntity;
import com.linus.es.demo.response.ResponseCode;
import com.linus.es.demo.response.ResponseResult;
import com.linus.es.demo.utils.ElasticUtil;
import com.linus.es.demo.vo.ElasticDataVO;
import com.linus.es.demo.vo.QueryVO;
import com.linus.es.demo.vo.SearchVO;
import com.linus.es.demo.vo.SimpleStringSearchVO;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * @author yuxuecheng
 * @Title: ElasticDataController
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:44
 */
@Slf4j
@RequestMapping("/elasticData/")
@RestController
public class ElasticDataController {

    /**
     * ElasticSearch查询时返回结果的最小评分
     */
    @Value("${es.min_score}")
    public float minScore;

    /**
     * ElasticSearch查询最大超时时间，单位ms
     */
    @Value("${es.search_timeout}")
    public int searchTimeout;

    @Autowired
    private BaseElasticDao baseElasticDao;

    @RequestMapping(value = "/add",method = RequestMethod.POST)
    public ResponseResult add(@RequestBody ElasticDataVO elasticDataVo){
        ResponseResult response = new ResponseResult<>();
        try {
            ElasticEntity elasticEntity = new ElasticEntity();
            elasticEntity.setId(elasticDataVo.getElasticEntity().getId());
            elasticEntity.setData(elasticDataVo.getElasticEntity().getData());

            baseElasticDao.insertOrUpdateOne(elasticDataVo.getIdxName(), elasticEntity);
        } catch (Exception e) {
            response.setCode(ResponseCode.ERROR.getCode());
            response.setMsg("服务忙，请稍后再试");
            response.setStatus(false);
            log.error("插入数据异常，metadataVo={},异常信息={}", elasticDataVo.toString(),e.getMessage());
        }
        return response;
    }

    @RequestMapping(value = "/get",method = RequestMethod.GET)
    public ResponseResult get(@RequestBody QueryVO queryVo){
        log.info("enter get");
        ResponseResult<List> response = new ResponseResult<>();
        try {
            Class<?> clazz = ElasticUtil.getClazz(queryVo.getClassName());
            Map<String,Object> params = queryVo.getQuery().get("match");
            Set<String> keys = params.keySet();
            MatchQueryBuilder queryBuilders=null;
            for(String ke:keys){
                queryBuilders = QueryBuilders.matchQuery(ke, params.get(ke));
            }

            if(null!=queryBuilders){
                SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(queryBuilders, 0, 10);
                List<?> data = baseElasticDao.search(queryVo.getIndexName(),searchSourceBuilder,clazz);
                response.setData(data);
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.ERROR.getCode());
            response.setMsg("服务忙，请稍后再试");
            response.setStatus(false);
            log.error("查询数据异常，metadataVo={},异常信息={}", queryVo.toString(),e.getMessage());
        }
        return response;
    }

    @RequestMapping(value = "/getById/{index}/{id}",method = RequestMethod.GET)
    public ResponseResult getById(@PathVariable("index") String index, @PathVariable("id") String id){
        log.info("enter get");
        ResponseResult<Map> response = new ResponseResult<>();
        try {
            GetResponse getResponse = baseElasticDao.getById(index, id);
            if (getResponse != null) {
                String returnIndex = getResponse.getIndex();
                String returnId = getResponse.getId();
                if (getResponse.isExists()) {
                    long version = getResponse.getVersion();
                    String sourceAsString = getResponse.getSourceAsString();
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    response.setData(sourceAsMap);
//                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();
                } else {
                    response.setMsg("数据不存在");
                    response.setCode(ResponseCode.RESOURCE_NOT_EXIST.getCode());
                }
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.ERROR.getCode());
            response.setMsg("服务忙，请稍后再试");
            response.setStatus(false);
            log.error("查询数据异常，metadataVo={},异常信息={}", id, e.getMessage());
        }
        return response;
    }

    @RequestMapping(value = "/search",method = RequestMethod.GET)
    public ResponseResult search(@RequestBody SearchVO searchVO){
        log.info("enter search");
        ResponseResult<String> response = new ResponseResult<>();
        try {
            QueryBuilder queryBuilder = ElasticUtil.parseQuery(searchVO.getQuery());
            if(null!=queryBuilder){
                SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(queryBuilder, 0, 10, searchTimeout, minScore);
                String result = baseElasticDao.search(searchVO.getIndexName(),searchSourceBuilder);
                response.setData(result);
            }
        } catch (Exception e) {
            response.setCode(ResponseCode.ERROR.getCode());
            response.setMsg("服务忙，请稍后再试");
            response.setStatus(false);
            log.error("查询数据异常，metadataVo={},异常信息={}", searchVO.toString(),e.getMessage());
        }
        return response;
    }

    @RequestMapping(value = "/simple_search",method = RequestMethod.GET)
    public ResponseResult simpleSearch(@RequestBody SimpleStringSearchVO simpleStringSearchVO){
        log.info("enter search");
        ResponseResult<String> response = new ResponseResult<>();
        try {
            QueryBuilder queryBuilder = QueryBuilders.simpleQueryStringQuery(simpleStringSearchVO.getQuery());
            SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(queryBuilder, 0, 10, searchTimeout, minScore);
            String result = baseElasticDao.search(simpleStringSearchVO.getIndexName(),searchSourceBuilder);
            response.setData(result);
        } catch (Exception e) {
            response.setCode(ResponseCode.ERROR.getCode());
            response.setMsg("服务忙，请稍后再试");
            response.setStatus(false);
            log.error("查询数据异常，metadataVo={},异常信息={}", simpleStringSearchVO.toString(),e.getMessage());
        }
        return response;
    }

    public ResponseResult getResponseResult(){
        return new ResponseResult();
    }
}
