package com.linus.es.demo;

import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.utils.ElasticQueryBuilderUtil;
import com.linus.es.demo.utils.ElasticUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author yuxuecheng
 * @Title: EsSearchTest
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/12 21:08
 */
@Slf4j
@Component
public class EsSearchTest implements CommandLineRunner {

    /**
     * ElasticSearch搜索时使用的分词器
     */
    @Value("${es.search_analyzer}")
    public String searchAnalyzer;

    /**
     * ElasticSearch搜索时使用的索引名称
     */
    @Value("${es.index_name}")
    public String indexName;

    @Autowired
    BaseElasticDao elasticDao;

    /**
     * 嵌入式插叙，查询诊断数据中诊断名称
     * @return
     */
    private String testDiagnosisDataSearch() {
        ElasticQueryBuilderUtil queryBuilderTools = new ElasticQueryBuilderUtil(searchAnalyzer);
        NestedQueryBuilder diagNestedQueryBuilder = queryBuilderTools.buildDiagnosisQueryBuilder("阵发性房颤");
//        MatchQueryBuilder chiefComplaintMatchQuery = QueryBuilders.matchQuery("chief_complaint", "胸闷");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(diagNestedQueryBuilder);
//        boolQueryBuilder.must(chiefComplaintMatchQuery);
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(boolQueryBuilder, 0, 100, 2000, 0.5f);
        String searchResult = elasticDao.searchReturnHits(indexName, searchSourceBuilder);
        log.info(searchResult);
        return searchResult;
    }

    /**
     * 嵌入式查询，查询手术数据中手术名称
     * @return
     */
    private String testOperationDataSearch() {
        ElasticQueryBuilderUtil queryBuilderTools = new ElasticQueryBuilderUtil(searchAnalyzer);
        NestedQueryBuilder operationNestedQueryBuilder = queryBuilderTools.buildOperationQueryBuilder("射频消融");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(operationNestedQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(boolQueryBuilder, 0, 100, 2000, 0.5f);
        String searchResult = elasticDao.searchReturnHits(indexName, searchSourceBuilder);
        log.info(searchResult);
        return searchResult;
    }

    /**
     * 嵌入式插叙，查询检验数据数据中检验名称和检验明细项名称
     * @return
     */
    private String testCheckDataSearch() {
        ElasticQueryBuilderUtil queryBuilderTools = new ElasticQueryBuilderUtil(searchAnalyzer);
        BoolQueryBuilder checkBoolQueryBuilder = queryBuilderTools.buildCheckQueryBuilder("红细胞压积");
        NestedQueryBuilder examQueryBuilder = queryBuilderTools.buildExamQueryBuilder("主动脉瓣少量返流");
        MatchQueryBuilder chiefComplaintMatchQuery = QueryBuilders.matchQuery("chief_complaint", "心悸");
        MatchQueryBuilder chiefComplaintNotMatchQuery = QueryBuilders.matchQuery("chief_complaint", "胸闷");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(checkBoolQueryBuilder);
        boolQueryBuilder.must(chiefComplaintMatchQuery);
        boolQueryBuilder.must(examQueryBuilder);
        boolQueryBuilder.mustNot(chiefComplaintNotMatchQuery);
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(boolQueryBuilder, 0, 100, 2000, 0.5f);
        String searchResult = elasticDao.searchReturnHits(indexName, searchSourceBuilder);
        log.info(searchResult);
        return searchResult;
    }

    /**
     *
     * @return
     */
    private String testMultiMatch(String searchText) {
        List<String> searchFileds = Arrays.asList("diagnosis_data.diagnosis_name", "chief_complaint", "chief_physician", "contact_addr", "check_data.detailName");
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchText, searchFileds.toArray(new String[0]));

        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(multiMatchQueryBuilder, 0, 100, 2000, 0.5f);
        String searchResult = elasticDao.searchReturnHits(indexName, searchSourceBuilder);
        log.info(searchResult);
        return searchResult;
    }

    /**
     *
     * @return 住院科室数量
     */
    private String testOfficeNumber() {
        MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        CardinalityAggregationBuilder dischargeDepAggr = AggregationBuilders.cardinality("discharge_dep_total");
        dischargeDepAggr.field("discharge_dep.raw");
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(queryBuilder, dischargeDepAggr, 0, 100, 2000, 0.5f);
        SearchResponse response = elasticDao.searchReturnRaw(indexName, searchSourceBuilder);
        Aggregations aggregations = response.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        for (Map.Entry<String, Aggregation> entry : aggregationMap.entrySet()) {

            if ("discharge_dep_total".equalsIgnoreCase(entry.getKey())) {
                ParsedCardinality parsedCardinality = (ParsedCardinality)entry.getValue();
                log.info("聚合项： " + entry.getKey() + "，值：" + parsedCardinality.getValue());
            }
        }
        return "";
    }

    /**
     *
     * @return 住院科室数量
     */
    private String testOfficeStat() {
        MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("discharge_dep_total");
        aggregationBuilder.field("discharge_dep.raw");
        aggregationBuilder.size(20);
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(queryBuilder, aggregationBuilder, 0, 100, 2000, 0.5f);
        SearchResponse response = elasticDao.searchReturnRaw(indexName, searchSourceBuilder);
        Aggregations aggregations = response.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        for (Map.Entry<String, Aggregation> entry : aggregationMap.entrySet()) {
            if ("discharge_dep_total".equalsIgnoreCase(entry.getKey())) {
                ParsedStringTerms parsedStringTerms = (ParsedStringTerms)entry.getValue();
                List<ParsedStringTerms.ParsedBucket> bucketList = (List<ParsedStringTerms.ParsedBucket>) parsedStringTerms.getBuckets();
                for (ParsedStringTerms.ParsedBucket bucket : bucketList) {
                    log.info("科室名称：" + bucket.getKeyAsString() + ", 患者数：" + bucket.getDocCount());
                }
            }
        }
        return "";
    }

    @Override
    public void run(String... args) throws Exception {
//        String result = nestedCheckDataSearch();
//        log.info(result);
//        String result = testOperationDataSearch();
//        log.info("手术查询结果：" + result);
//        String result = testOfficeStat();
//        log.info("科室查询结果：" + result);
//        String result = testDiagnosisDataSearch();
//        log.info("手术查询结果：" + result);
    }
}
