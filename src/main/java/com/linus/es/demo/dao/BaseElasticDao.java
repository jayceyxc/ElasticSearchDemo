package com.linus.es.demo.dao;

import com.alibaba.fastjson.JSON;
import com.linus.es.demo.entity.ElasticEntity;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * @author yuxuecheng
 * @Title: BaseElasticDao
 * @ProjectName demo
 * @Description: 操作Elastic数据库
 * @date 2019-11-02 15:56
 */
@Slf4j
@Component
@Order(value = 1)
public class BaseElasticDao {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 设置分片
     * @param request
     */
    private void buildSetting(CreateIndexRequest request){

        request.settings(Settings.builder().put("index.number_of_shards",3)
                .put("index.number_of_replicas",2));
    }

    /**
     * 创建ES索引
     * @param indexName 索引名称
     * @param indexMapping 索引描述
     */
    public boolean createIndex(String indexName, String indexMapping) {
        try {
            boolean result = this.indexExist(indexName);
            if (result) {
                log.error(" indexName={} 已经存在,indexSql={}",indexName,indexMapping);
                return true;
            }
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            buildSetting(request);
            //可选参数
            //超时,等待所有节点被确认(使用TimeValue方式)
            request.setTimeout(TimeValue.timeValueMinutes(2));

            //连接master节点的超时时间(使用TimeValue方式)
            request.setMasterTimeout(TimeValue.timeValueMinutes(1));

            //在创建索引API返回响应之前等待的活动分片副本的数量，以ActiveShardCount形式表示。
//            request.waitForActiveShards(ActiveShardCount.from(2));
            //request.waitForActiveShards(ActiveShardCount.DEFAULT);

            request.mapping(indexMapping, XContentType.JSON);
            // 同步执行
            CreateIndexResponse res = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            // 返回的CreateIndexResponse允许检索有关执行的操作的信息
            if (!res.isAcknowledged() || !res.isShardsAcknowledged()) {
                log.error("创建索引失败：{}", indexName);
                return false;
            }
            return true;
            // 异步执行
            //异步执行创建索引请求需要将CreateIndexRequest实例和ActionListener实例传递给异步方法：
            //CreateIndexResponse的典型监听器如下所示：
            //异步方法不会阻塞并立即返回。
//            ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
//                @Override
//                public void onResponse(CreateIndexResponse createIndexResponse) {
//                    log.info("onResponse");
//                    //如果执行成功，则调用onResponse方法;
//                    if (!createIndexResponse.isAcknowledged() || !createIndexResponse.isShardsAcknowledged()) {
//                        throw new RuntimeException("初始化失败");
//                    }
//                }
//                @Override
//                public void onFailure(Exception e) {
//                    log.info("onFailure");
//                    //如果失败，则调用onFailure方法。
//                    log.error(e.getMessage());
//                }
//            };

            //要执行的CreateIndexRequest和执行完成时要使用的ActionListener
//            restHighLevelClient.indices().createAsync(request, RequestOptions.DEFAULT, listener);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 获取指定索引的映射
     * @param idxName 索引名称
     * @return
     * @throws Exception
     */
    public GetMappingsResponse getMappingByIndexName(String idxName) throws Exception {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(idxName);
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引是否存在
     * @param idxName 索引名称
     * @return
     * @throws Exception
     */
    public boolean indexExist(String idxName) throws Exception {
        GetIndexRequest request = new GetIndexRequest(idxName);
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);

//        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        boolean resp = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        log.info("索引" + idxName + "是否存在：" + resp);
        return resp;
    }

    /**
     * 删除索引
     * @param idxName 索引名称
     */
    public boolean deleteIndex(String idxName) {
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(idxName);
            // 设置超时，等待所有节点确认索引删除（使用TimeValue形式）
            deleteIndexRequest.timeout(TimeValue.timeValueMinutes(1));
            // 连接master节点的超时时间(使用TimeValue方式)
            deleteIndexRequest.masterNodeTimeout(TimeValue.timeValueMinutes(2));
            // 设置IndicesOptions控制如何解决不可用的索引以及如何扩展通配符表达式
//            deleteIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

            AcknowledgedResponse response = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new RuntimeException("删除索引失败");
            } else {
                log.info("删除索引成功：" + idxName);
            }
            return true;
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                log.warn("要删除的索引不存在: " + idxName);
            }
            return false;
        } catch (IOException ioe) {
            log.error(ioe.getMessage());
            return false;
        }
    }

    /**
     * 索引别名
     * @param oldIndexName 原索引名字
     * @param aliasIndexName 索引别名
     * @return
     */
    public boolean aliasIndex(String oldIndexName, String aliasIndexName) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(oldIndexName).alias(aliasIndexName);
        request.addAliasAction(aliasActions);
        try {
            AcknowledgedResponse response = restHighLevelClient.indices().updateAliases(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                log.error("创建索引别名失败。索引名称：" + oldIndexName + ", 索引别名：" + aliasIndexName);
                return false;
            } else {
                log.info("创建索引别名成功。索引名称：" + oldIndexName + ", 索引别名：" + aliasIndexName);
            }
            return true;
        } catch (ElasticsearchException ee) {
            log.error("创建索引别名失败", ee);
            return false;
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
            return false;
        }
    }

    /**
     * 插入或更新记录
     * @param idxName
     * @param entity
     */
    public void insertOrUpdateOne(String idxName, ElasticEntity entity) {

        IndexRequest request = new IndexRequest(idxName);
        log.info("Data : id={},entity={}",entity.getId(), JSON.toJSONString(entity.getData()));
        request.id(entity.getId());
        request.source(JSON.toJSONString(entity.getData()), XContentType.JSON);
        try {
            restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException ee) {
            log.error("数据插入失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }
    }

    /**
     * 批量插入输入
     * @param idxName 索引名称
     * @param list 待插入数据列表
     */
    public void insertBatch(String idxName, List<ElasticEntity> list) {

        BulkRequest request = new BulkRequest();
        list.forEach(item -> request.add(new IndexRequest(idxName).id(item.getId())
                .source(JSON.toJSONString(item.getData()), XContentType.JSON)));
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse response : bulkResponse) {
                log.info(response.getFailureMessage());
                log.info(response.getId());
            }
        } catch (ElasticsearchException ee) {
            log.error("数据插入失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }
    }

    /**
     * 批量删除
     * @param idxName 索引名称
     * @param idList 待删除数据的ID列表
     * @param <T>
     */
    public <T> void deleteBatch(String idxName, Collection<T> idList) {

        BulkRequest request = new BulkRequest();
        idList.forEach(item -> request.add(new DeleteRequest(idxName, item.toString())));
        try {
            restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException ee) {
            log.error("数据插入失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }
    }

    /**
     * 根据查询条件删除记录
     * @param idxName 索引名称
     * @param builder 查询条件
     */
    public void deleteByQuery(String idxName, QueryBuilder builder) {

        DeleteByQueryRequest request = new DeleteByQueryRequest(idxName);
        request.setQuery(builder);
        //设置批量操作数量,最大为10000
        request.setBatchSize(10000);
        request.setConflicts("proceed");
        try {
            restHighLevelClient.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException ee) {
            log.error("数据删除失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }
    }

    public GetResponse getById(String idxName, String id) {
        GetRequest request = new GetRequest(idxName, id);
        // Configure source inclusion for specific fields
//      String[] includes = new String[]{"message", "*Date"};
//      String[] excludes = Strings.EMPTY_ARRAY;
//      FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
//      request.fetchSourceContext(fetchSourceContext);
        // Configure source exclusion for specific fields
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"message"};
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        // Preference value
        request.preference("preference");
        // Set realtime flag to false (true by default)
        request.realtime(false);
        // Perform a refresh before retrieving the document (false by default)
        request.refresh(true);
        // Version
//        request.version(2);
        // Version type
//        request.versionType(VersionType.EXTERNAL);
        try {
            // 同步执行
            GetResponse getResponse = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            log.info(getResponse.getId());

            return getResponse;
        } catch (ElasticsearchException ee) {
            if (ee.status() == RestStatus.NOT_FOUND) {
                log.info("记录不存在");
            } else {
                log.error(ee.getDetailedMessage());
            }
        } catch (IOException ioe) {
            log.error(ioe.getMessage());
        }

        return null;
    }

    /**
     * 查询
     * @param idxName 索引名称
     * @param builder 查询参数
     * @param c 结果类对象
     * @param <T> 类型
     * @return
     */
    public <T> List<T> searchReturnHits(String idxName, SearchSourceBuilder builder, Class<T> c) {

        SearchRequest request = new SearchRequest(idxName);
        request.source(builder);
        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            List<T> res = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                res.add(JSON.parseObject(hit.getSourceAsString(), c));
            }
            return res;
        } catch (ElasticsearchException ee) {
            log.error("数据搜索失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }
        return null;
    }

    /**
     * 查询
     * @param idxName 索引名称
     * @param builder 查询参数
     * @return
     */
    public String searchReturnHits(String idxName, SearchSourceBuilder builder) {
        SearchRequest request = new SearchRequest(idxName);
        request.source(builder);
        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            log.info("total hits: {}", response.getHits().getTotalHits().value);
            List<Map<String, Object>> returnResult = new ArrayList<>();
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                log.info("评分：{}", hit.getScore());
                log.info(JSON.toJSONString(hit.getSourceAsMap()));
                returnResult.add(hit.getSourceAsMap());
            }
            return JSON.toJSONString(returnResult);
        } catch (ElasticsearchException ee) {
            log.error("数据搜索失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }

        return null;
    }

    /**
     * 查询
     * @param idxName 索引名称
     * @param builder 查询参数
     * @return
     */
    public SearchResponse searchReturnRaw(String idxName, SearchSourceBuilder builder) {
        SearchRequest request = new SearchRequest(idxName);
        request.source(builder);
        try {
            return restHighLevelClient.search(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException ee) {
            log.error("数据搜索失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }

        return null;
    }

    /**
     * 多条件查询
     * @param idxName 索引名称
     * @param sourceBuilderList 查询参数列表
     * @return
     */
    public String searchReturnHits(String idxName, List<SearchSourceBuilder> sourceBuilderList) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (SearchSourceBuilder sourceBuilder : sourceBuilderList) {
            SearchRequest request = new SearchRequest(idxName);
            request.source(sourceBuilder);
            multiSearchRequest.add(request);
        }
        try {
            List<SearchHit> result = new ArrayList<>();
            MultiSearchResponse multiResponse = restHighLevelClient.msearch(multiSearchRequest, RequestOptions.DEFAULT);
            for (Iterator<MultiSearchResponse.Item> it = multiResponse.iterator(); it.hasNext(); ) {
                MultiSearchResponse.Item item = it.next();
                if (item.getFailure() != null) {
                    SearchResponse response = item.getResponse();
                    log.info("total hits: {}", response.getHits().getTotalHits().value);
                    SearchHit[] hits = response.getHits().getHits();
                    for (SearchHit hit : hits) {
                        log.info(JSON.toJSONString(hit.getSourceAsMap()));
                        result.add(hit);
                    }
                }
            }
            return JSON.toJSONString(result);
        } catch (ElasticsearchException ee) {
            log.error("数据搜索失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }

        return null;
    }
}
