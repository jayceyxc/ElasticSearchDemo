package com.linus.es.demo.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author yuxuecheng
 * @Title: ElasticQueryBuilderUtil
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/19 17:38
 */
@Slf4j
public class ElasticQueryBuilderUtil {

    private String searchAnalyzer;

    public ElasticQueryBuilderUtil() {
        this.searchAnalyzer = "ik_smart";
    }

    public ElasticQueryBuilderUtil(String searchAnalyzer) {
        this.searchAnalyzer = searchAnalyzer;
    }

    /**
     * 创建嵌入式查询对象
     * @param path 路径
     * @param field 查询字段
     * @param searchText 搜索文本
     * @return 嵌入式查询
     */
    private NestedQueryBuilder buildNestedQueryBuilder(String path, String field, String searchText) {
        BoolQueryBuilder nestedBoolQueryBuilder = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, searchText);
        matchQueryBuilder.analyzer(searchAnalyzer);
        nestedBoolQueryBuilder.must(matchQueryBuilder);
        NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(path, nestedBoolQueryBuilder, ScoreMode.Max);
        log.info("returned query: " + nestedBoolQueryBuilder);

        return nestedQueryBuilder;
    }

    /**
     * 创建嵌入式查询对象
     * @param path 路径
     * @param fields 查询字段列表
     * @param searchText 搜索文本
     * @return 嵌入式查询
     */
    private NestedQueryBuilder buildNestedQueryBuilder(String path, List<String> fields, String searchText) {
        BoolQueryBuilder nestedBoolQueryBuilder = QueryBuilders.boolQuery();
        for (String field : fields) {
            MatchPhrasePrefixQueryBuilder phrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery(field, searchText);
            phrasePrefixQueryBuilder.analyzer(searchAnalyzer);
            nestedBoolQueryBuilder.should(phrasePrefixQueryBuilder);
        }
        NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(path, nestedBoolQueryBuilder, ScoreMode.Max);
        log.info("returned query: " + nestedBoolQueryBuilder);

        return nestedQueryBuilder;
    }

    /**
     * 构建诊断名称查询对象
     * @param searchText 搜索文本
     * @return 诊断名称嵌入式查询
     */
    public NestedQueryBuilder buildDiagnosisQueryBuilder(String searchText) {
        return buildNestedQueryBuilder("diagnosis_data", "diagnosis_data.diagnosis_name", searchText);
    }

    /**
     * 构建手术名称、术者姓名查询对象
     * @param searchText 搜索文本
     * @return 诊断名称嵌入式查询
     */
    public NestedQueryBuilder buildOperationQueryBuilder(String searchText) {
        return buildNestedQueryBuilder("operation_data", Arrays.asList("operation_data.operation_name", "operation_data.operator"), searchText);
    }

    /**
     * 构建检验名称查询对象
     * @param searchText 搜索文本
     * @return 诊断名称嵌入式查询
     */
    public BoolQueryBuilder buildCheckQueryBuilder(String searchText) {
        NestedQueryBuilder checkNameQueryBuilder = buildNestedQueryBuilder("check_data", "check_data.name", searchText);
        BoolQueryBuilder checkBoolQueryBuilder = QueryBuilders.boolQuery();
        NestedQueryBuilder checkDetailNameQueryBuilder = buildNestedQueryBuilder("check_data.detailItemList", "check_data.detailItemList.detailName", searchText);
        checkBoolQueryBuilder.should(checkNameQueryBuilder);
        checkBoolQueryBuilder.should(checkDetailNameQueryBuilder);

        return checkBoolQueryBuilder;
    }

    /**
     * 构建检查名称查询对象
     * @param searchText 搜索文本
     * @return 诊断名称嵌入式查询
     */
    public NestedQueryBuilder buildExamQueryBuilder(String searchText) {
        return buildNestedQueryBuilder("examination_data", Arrays.asList("examination_data.image_direction",
                "examination_data.examination_category_code", "examination_data.examination_result"), searchText);
    }

    /**
     * 构建医嘱名称查询对象
     * @param searchText 搜索文本
     * @return 诊断名称嵌入式查询
     */
    public NestedQueryBuilder buildTreatmentQueryBuilder(String searchText) {
        return buildNestedQueryBuilder("treatment_data", Arrays.asList("examination_data.image_direction",
                "examination_data.examination_category_code", "examination_data.examination_result"), searchText);
    }
}
