package com.linus.es.demo.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.linus.es.demo.entity.BatchInsertResult;
import com.linus.es.demo.entity.ElasticEntity;
import com.linus.es.demo.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
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
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    /**
     * ElasticSearch创建索引时默认使用的属性配置文件
     */
    @Value("${es.properties_json}")
    public String propertiesJson;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 设置分片
     * @param request
     */
    private void buildSetting(CreateIndexRequest request){

        Settings.Builder builder = Settings.builder();
        builder.put("index.number_of_shards",3);
        builder.put("index.number_of_replicas",2);

        request.settings(builder);
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
     * 根据配置文件index_properties.json配置的属性创建索引
     * @param indexName 索引名称
     * @return
     */
    public boolean createDefaultIndex(String indexName) {
        return createDefaultIndex(indexName, propertiesJson);
    }

    /**
     * 根据配置文件index_properties.json配置的属性创建索引
     * @param indexName 索引名称
     * @return
     */
    public boolean createDefaultIndex(String indexName, String jsonPropertiesFile) {
        JSONObject jsonObject = JsonUtil.getJsonObjFromResource(jsonPropertiesFile);
        String jsonProperties = jsonObject.toJSONString();
        log.warn("indexName={}, indexSql={}", indexName, jsonProperties);
        return createIndex(indexName, jsonProperties);
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
     * 删除索引别名
     * @param oldIndexName 原索引名字
     * @param aliasIndexName 索引别名
     * @return
     */
    public boolean deleteAliasIndex(String oldIndexName, String aliasIndexName) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index(oldIndexName).alias(aliasIndexName);
        request.addAliasAction(aliasActions);
        try {
            AcknowledgedResponse response = restHighLevelClient.indices().updateAliases(request, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                log.error("删除索引别名失败。索引名称：" + oldIndexName + ", 索引别名：" + aliasIndexName);
                return false;
            } else {
                log.info("删除索引别名成功。索引名称：" + oldIndexName + ", 索引别名：" + aliasIndexName);
            }
            return true;
        } catch (ElasticsearchException ee) {
            log.error("删除索引别名失败", ee);
            return false;
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
            return false;
        }
    }

    /**
     * 获取索引别名相关信息
     * @param aliasIndexName 索引别名
     * @return
     */
    public String getAliasIndex(String aliasIndexName) {
        String result = null;
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases(aliasIndexName);
        try {
            GetAliasesResponse response = restHighLevelClient.indices().getAlias(request, RequestOptions.DEFAULT);
            if (response.status() == RestStatus.OK) {
                log.error("查询索引别名成功。索引别名：" + aliasIndexName);
                Map<String, Set<AliasMetaData>> aliases = response.getAliases();
                if (aliases == null || aliases.size() == 0) {
                    log.info("索引别名不存在。索引别名：" + aliasIndexName);
                    return result;
                }
                if (aliases.size() > 1) {
                    log.info("索引别名对应的索引有多个，请手动确认。索引别名：" + aliasIndexName);
                    return result;
                }

                for (Map.Entry<String, Set<AliasMetaData>> entry : aliases.entrySet()) {
                    Set<AliasMetaData> aliasMetaDataSet = entry.getValue();
                    for (AliasMetaData metaData : aliasMetaDataSet) {
                        if (metaData.getAlias().equalsIgnoreCase(aliasIndexName)) {
                            result = entry.getKey();
                        }
                    }
                }
            } else {
                log.info("查询索引别名失败。索引别名：" + aliasIndexName);
            }
        } catch (ElasticsearchException ee) {
            log.error("查询索引别名失败", ee);
            return result;
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
            return result;
        }

        return result;
    }

    /**
     * 重建索引
     * @param sourceIndex 原索引名称
     * @param destIndex 目标索引名称
     * @return
     */
    public boolean reindex(String sourceIndex, String destIndex) {
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex);
        // versionType默认是INTERNAL，ElasticSearch会盲目的将文档拷贝到目标index
        // 设置为EXTERNAL，则ElasticSearch会维持原索引中的版本，目标索引中没有的文档会新建，目标索引中的文档版本比原索引中
        // 版本号老的会更新
        request.setDestVersionType(VersionType.INTERNAL);
        // 可以设置proceed或abort
        request.setConflicts("proceed");
        request.setRefresh(true);
        try {
            log.info("开始索引重建：原索引：" + sourceIndex + "，目标索引：" + destIndex);
            long startTime = System.currentTimeMillis();
            BulkByScrollResponse bulkResponse = restHighLevelClient.reindex(request, RequestOptions.DEFAULT);
            long finishedTime = System.currentTimeMillis();
            log.info("完成索引重建：原索引：" + sourceIndex + "，目标索引：" + destIndex + "，共耗时：" + (finishedTime - startTime) + "ms");
            log.info(bulkResponse.getTook().toString());
            log.info("总文档数：" + bulkResponse.getTotal());
            log.info("更新文档数：" + bulkResponse.getUpdated());
            log.info("新建文档数：" + bulkResponse.getCreated());
            log.info("删除文档数：" + bulkResponse.getDeleted());
            return true;
        } catch (ElasticsearchException ee) {
            log.error("重建索引失败", ee);
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
    public BatchInsertResult insertBatch(String idxName, List<ElasticEntity> list) {

        BulkRequest request = new BulkRequest();
        list.forEach(item -> request.add(new IndexRequest(idxName).id(item.getId())
                .source(JSON.toJSONString(item.getData()), XContentType.JSON)));
        try {
            int successNum = 0;
            int failureNum = 0;
            BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse response : bulkResponse) {
                if (!response.isFailed()) {
                    log.info("插入成功。索引名称：" + idxName + "，数据ID：" + response.getId() + "， 更新结果：" + response.getResponse().getResult().name());
                    successNum++;
                } else {
                    log.error("插入失败。索引名称：" + idxName + "，数据ID：" + response.getId() + "，失败原因：" + response.getFailureMessage());
                    failureNum++;
                }
            }
            log.info("索引名称：" + idxName + "，插入成功 " + successNum + "条，插入失败：" + failureNum + "条。");
            BatchInsertResult result =  new BatchInsertResult();
            result.setFailedCount(failureNum);
            result.setSuccessCount(successNum);

            return result;
        } catch (ElasticsearchException ee) {
            log.error("数据插入失败", ee);
        } catch (IOException ioe) {
            log.error("IO异常", ioe);
        }

        return new BatchInsertResult();
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
            log.error("数据搜索失败，原因：" + ee.getDetailedMessage(), ee);
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
