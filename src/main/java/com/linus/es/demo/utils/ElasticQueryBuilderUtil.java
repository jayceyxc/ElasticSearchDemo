package com.linus.es.demo.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.*;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

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
        MatchPhrasePrefixQueryBuilder phrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery(field, searchText);
        phrasePrefixQueryBuilder.analyzer(searchAnalyzer);
        nestedBoolQueryBuilder.should(phrasePrefixQueryBuilder);
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

    /**
     * 构建其他文本的查询对象
     * @param searchText 搜索文本
     * @return 其他文本布尔查询
     */
    public MultiMatchQueryBuilder buildOtherTextQueryBuilder(String searchText, String fields) {
        List<String> searchFileds = Arrays.asList(fields.split(","));
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(searchText, searchFileds.toArray(new String[0]));
        multiMatchQueryBuilder.type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX);

        return multiMatchQueryBuilder;
    }

    /**
     * 搜索手游文本的查询对象
     * @param searchText 搜索文本
     * @return bool查询对象
     */
    public BoolQueryBuilder buildAllQueryBuilder(String searchText, String fields) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 搜索检验
        BoolQueryBuilder checkBoolQueryBuilder = buildCheckQueryBuilder(searchText);
        // 搜索检查
        NestedQueryBuilder examQueryBuilder = buildExamQueryBuilder(searchText);
        // 搜索医嘱
        NestedQueryBuilder treatmentQueryBuilder = buildTreatmentQueryBuilder(searchText);
        // 搜索诊断
        NestedQueryBuilder diagnosisQueryBuilder = buildDiagnosisQueryBuilder(searchText);
        // 搜索手术
        NestedQueryBuilder operationQueryBuilder = buildOperationQueryBuilder(searchText);
        // 搜索全文
        MultiMatchQueryBuilder multiMatchQueryBuilder = buildOtherTextQueryBuilder(searchText, fields);

        boolQueryBuilder.should(checkBoolQueryBuilder);
        boolQueryBuilder.should(examQueryBuilder);
        boolQueryBuilder.should(treatmentQueryBuilder);
        boolQueryBuilder.should(diagnosisQueryBuilder);
        boolQueryBuilder.should(operationQueryBuilder);
        boolQueryBuilder.should(multiMatchQueryBuilder);

        return boolQueryBuilder;
    }

    /**
     * 搜索手游文本的查询对象
     * @param searchText 搜索文本
     * @return bool查询对象
     */
    public BoolQueryBuilder buildBoolQueryBuilder(Queue<String> searchText, String fields) throws IllegalArgumentException {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        String keyword = "";
        while(!searchText.isEmpty()) {
            String word = searchText.peek();
            if (!word.matches("[\\$\\&\\!\\|\\(\\)]")) {
                searchText.poll();
                keyword = word;
            } else if (word.equalsIgnoreCase("&")) {
                searchText.poll();
                BoolQueryBuilder subQueryBuilder = buildAllQueryBuilder(keyword, fields);
                boolQueryBuilder.must(subQueryBuilder);
            } else if (word.equalsIgnoreCase("|")) {
                searchText.poll();
                BoolQueryBuilder subQueryBuilder = buildAllQueryBuilder(keyword, fields);
                boolQueryBuilder.should(subQueryBuilder);
            } else if (word.equalsIgnoreCase("!")) {
                searchText.poll();
                BoolQueryBuilder subQueryBuilder = buildAllQueryBuilder(keyword, fields);
                boolQueryBuilder.mustNot(subQueryBuilder);
            } else if (word.equalsIgnoreCase("(")) {
                searchText.poll();
                // 如果碰到左括号的则左括号的部分需要进行嵌入处理，并根据右括号后面的运算符和当前的运算进行合并
                BoolQueryBuilder subQueryBuilder = buildBoolQueryBuilder(searchText, fields);
                if (searchText.isEmpty() || !(searchText.peek().equalsIgnoreCase(")"))) {
                    log.info("布尔搜索字符串格式错误");
                    throw new IllegalArgumentException("布尔搜索字符串格式错误");
                } else {
                    // 弹出右括号
                    searchText.poll();
                    if (searchText.isEmpty() || !(searchText.peek().matches("[\\!\\&\\|]"))) {
                        log.info("布尔搜索字符串格式错误");
                        throw new IllegalArgumentException("布尔搜索字符串格式错误");
                    }
                    String operator = searchText.poll();
                    if (operator.equalsIgnoreCase("&")) {
                        boolQueryBuilder.must(subQueryBuilder);
                    } else if (operator.equalsIgnoreCase("|")) {
                        boolQueryBuilder.should(subQueryBuilder);
                    } else if (operator.equalsIgnoreCase("!")) {
                        boolQueryBuilder.mustNot(subQueryBuilder);
                    } else {
                        log.info("布尔搜索字符串格式错误");
                        throw new IllegalArgumentException("布尔搜索字符串格式错误");
                    }
                }
            } else if (word.equalsIgnoreCase(")")) {
                // 碰到右括号则跳出循环，
                break;
            }
        }
        return boolQueryBuilder;
    }
}
