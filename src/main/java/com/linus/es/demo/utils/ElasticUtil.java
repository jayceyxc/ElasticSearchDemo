package com.linus.es.demo.utils;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author yuxuecheng
 * @Title: ElasticUtil
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 16:45
 */
@Slf4j
public class ElasticUtil {
    public static Class<?> getClazz(String clazzName) {
        try {
            return Class.forName(clazzName);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    /**
     * 解析query中的Map
     *
     * @param queryMap
     * @return
     */
    private static List<QueryBuilder> parseQueryMap(LinkedHashMap<String, Object> queryMap) {
        List<QueryBuilder> queryBuilderList = new ArrayList<>();
        for (String innerKey : queryMap.keySet()) {
            if (MatchQueryBuilder.NAME.equalsIgnoreCase(innerKey)) {
                Object matchValue = queryMap.get(innerKey);
                if (matchValue instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> matchQueryMap = (LinkedHashMap<String, Object>) matchValue;
                    for (Map.Entry<String, Object> entry : matchQueryMap.entrySet()) {
                        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(entry.getKey(), entry.getValue());
                        matchQueryBuilder.operator(Operator.AND);
                        queryBuilderList.add(matchQueryBuilder);
                    }
                }
            } else if (TermQueryBuilder.NAME.equalsIgnoreCase(innerKey)) {
                Object termValue = queryMap.get(innerKey);
                if (termValue instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> termQueryMap = (LinkedHashMap<String, Object>) termValue;
                    for (Map.Entry<String, Object> entry : termQueryMap.entrySet()) {
                        queryBuilderList.add(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
                    }
                }
            } else if (RangeQueryBuilder.NAME.equalsIgnoreCase(innerKey)) {
                Object rangeValue = queryMap.get(innerKey);
                if (rangeValue instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> rangeQueryMap = (LinkedHashMap<String, Object>) rangeValue;
                    for (String field : rangeQueryMap.keySet()) {
                        Object fieldRangeQuery = rangeQueryMap.get(field);
                        if (fieldRangeQuery instanceof LinkedHashMap) {
                            LinkedHashMap<String, Object> fieldQueryMap = (LinkedHashMap<String, Object>) fieldRangeQuery;
                            for (String rangeKey : fieldQueryMap.keySet()) {
                                if (RangeQueryBuilder.GT_FIELD.getPreferredName().equalsIgnoreCase(rangeKey)) {
                                    queryBuilderList.add(QueryBuilders.rangeQuery(field).gt(fieldQueryMap.get(rangeKey)));
                                } else if (RangeQueryBuilder.GTE_FIELD.getPreferredName().equalsIgnoreCase(rangeKey)) {
                                    queryBuilderList.add(QueryBuilders.rangeQuery(field).gte(fieldQueryMap.get(rangeKey)));
                                } else if (RangeQueryBuilder.LT_FIELD.getPreferredName().equalsIgnoreCase(rangeKey)) {
                                    queryBuilderList.add(QueryBuilders.rangeQuery(field).lt(fieldQueryMap.get(rangeKey)));
                                } else if (RangeQueryBuilder.LTE_FIELD.getPreferredName().equalsIgnoreCase(rangeKey)) {
                                    queryBuilderList.add(QueryBuilders.rangeQuery(field).lte(fieldQueryMap.get(rangeKey)));
                                }
                            }
                        }
                    }
                }
            }
        }
        return queryBuilderList;
    }

    /**
     * 解析must、should等的子查询
     *
     * @param query
     * @return
     */
    private static List<QueryBuilder> parseSubQuery(Object query) {
        List<QueryBuilder> queryBuilderList = new ArrayList<>();
        if (query instanceof LinkedHashMap) {
            LinkedHashMap<String, Object> mustQueryMap = (LinkedHashMap<String, Object>) query;
            return parseQueryMap(mustQueryMap);
        } else if (query instanceof ArrayList) {
            ArrayList mustQueryList = (ArrayList) query;
            for (Object object : mustQueryList) {
                if (object instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> mustQueryMap = (LinkedHashMap<String, Object>) object;
                    List<QueryBuilder> queryBuilders = parseQueryMap(mustQueryMap);
                    queryBuilderList.addAll(queryBuilders);
                }
            }
        }
        return queryBuilderList;
    }

    /**
     * 解析查询对象
     *
     * @param query 查询对象
     * @return
     */
    public static QueryBuilder parseQuery(Map<String, Map<String, Object>> query) {
        if (query == null || query.isEmpty()) {
            return null;
        }

        QueryBuilder queryBuilder = null;
        if (query.containsKey(BoolQueryBuilder.NAME)) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            Map<String, Object> subQuery = query.get(BoolQueryBuilder.NAME);
            for (String key : subQuery.keySet()) {
                if ("must".equalsIgnoreCase(key)) {
                    Object value = subQuery.get(key);
                    List<QueryBuilder> queryBuilders = parseSubQuery(value);
                    for (QueryBuilder queryBuilder1 : queryBuilders) {
                        boolQueryBuilder.must(queryBuilder1);
                    }
                } else if ("should".equalsIgnoreCase(key)) {
                    Object value = subQuery.get(key);
                    List<QueryBuilder> queryBuilders = parseSubQuery(value);
                    for (QueryBuilder queryBuilder1 : queryBuilders) {
                        boolQueryBuilder.should(queryBuilder1);
                    }
                } else if ("must_not".equalsIgnoreCase(key) || "mustNot".equalsIgnoreCase(key)) {
                    Object value = subQuery.get(key);
                    List<QueryBuilder> queryBuilders = parseSubQuery(value);
                    for (QueryBuilder queryBuilder1 : queryBuilders) {
                        boolQueryBuilder.mustNot(queryBuilder1);
                    }
                } else if ("filter".equalsIgnoreCase(key)) {
                    Object value = subQuery.get(key);
                    List<QueryBuilder> queryBuilders = parseSubQuery(value);
                    for (QueryBuilder queryBuilder1 : queryBuilders) {
                        boolQueryBuilder.filter(queryBuilder1);
                    }
                }
            }
            queryBuilder = boolQueryBuilder;
        } else if (query.containsKey(TermQueryBuilder.NAME)) {
            Map<String, Object> termParams = query.get("term");
            if (termParams != null) {
                Set<String> keys = termParams.keySet();
                TermQueryBuilder queryBuilders = null;
                for (String ke : keys) {
                    queryBuilders = QueryBuilders.termQuery(ke, termParams.get(ke));
                }
                queryBuilder = queryBuilders;
            }
        } else if (query.containsKey(MatchQueryBuilder.NAME)) {
            Map<String, Object> matchParams = query.get("match");
            if (matchParams != null) {
                Set<String> keys = matchParams.keySet();
                MatchQueryBuilder queryBuilders = null;
                for (String key : keys) {
                    MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, matchParams.get(key));
                    // 这里AND和OR的区别是：AND要求查询记录符合查询字符串分词后所得的所有词语，OR则只需要符合任何一个即可。
//                    matchQueryBuilder.operator(Operator.OR);
                    matchQueryBuilder.operator(Operator.AND);
                    queryBuilders = matchQueryBuilder;
                }
                queryBuilder = queryBuilders;
            }
        }

        return queryBuilder;
    }

    /**
     * @param queryBuilder 设置查询对象
     * @param from         设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size         设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initSearchSourceBuilder(QueryBuilder queryBuilder, int from, int size) {
        return initSearchSourceBuilder(queryBuilder, null, from, size, 2000, 0.65f);
    }

    /**
     * @param queryBuilder 设置查询对象
     * @param from         设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size         设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initSearchSourceBuilder(QueryBuilder queryBuilder, int from, int size, int timeout, float minScore) {
        return initSearchSourceBuilder(queryBuilder, null, from, size, timeout, minScore);
    }

    /**
     * @param queryBuilder 设置查询对象
     * @param from         设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size         设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initSearchSourceBuilder(QueryBuilder queryBuilder, AggregationBuilder aggregationBuilder, int from, int size, int timeout) {
        return initSearchSourceBuilder(queryBuilder, aggregationBuilder, from, size, timeout, 0.65f);
    }

    /**
     * @param queryBuilder 设置查询对象
     * @param from         设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size         设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initSearchSourceBuilder(QueryBuilder queryBuilder, AggregationBuilder aggregationBuilder, int from, int size, int timeout,
                                                              float minScore) {
        return initSearchSourceBuilder(queryBuilder, aggregationBuilder, from, size, timeout, minScore, Arrays.asList("demography_xm", "zyks", "zzys"));
    }

    /**
     * @param queryBuilder 设置查询对象
     * @param from         设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size         设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initSearchSourceBuilder(QueryBuilder queryBuilder, AggregationBuilder aggregationBuilder,
                                                              int from, int size, int timeout, float minScore, List<String> highlightFields) {

        //使用默认选项创建 SearchSourceBuilder 。
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置查询对象。可以使任何类型的 QueryBuilder
        sourceBuilder.query(queryBuilder);
        if (aggregationBuilder != null) {
            sourceBuilder.aggregation(aggregationBuilder);
        }
        //设置from选项，确定要开始搜索的结果索引。 默认为0。
        sourceBuilder.from(from);
        //设置大小选项，确定要返回的搜索匹配数。 默认为10。
        sourceBuilder.size(size);
        // 设置最小评分
        sourceBuilder.minScore(minScore);
        // 返回版本号
        sourceBuilder.version(true);
        // 设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        if (highlightFields != null) {
            for (String field : highlightFields) {
                highlightBuilder.field(field);
            }
        }
        sourceBuilder.highlighter(highlightBuilder);
        sourceBuilder.timeout(new TimeValue(timeout, TimeUnit.MILLISECONDS));
        sourceBuilder.sort("_score", SortOrder.ASC);

        log.info("search source builder: " + sourceBuilder);
        return sourceBuilder;
    }

    /**
     * @param aggregationBuilder 设置聚合对象
     * @param from               设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size               设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initAggregationSourceBuilder(AggregationBuilder aggregationBuilder, int from, int size, int timeout,
                                                                   float minScore, List<String> highlightFields) {

        //使用默认选项创建 SearchSourceBuilder 。
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置查询对象。可以使任何类型的 QueryBuilder
        sourceBuilder.aggregation(aggregationBuilder);
        //设置from选项，确定要开始搜索的结果索引。 默认为0。
        sourceBuilder.from(from);
        //设置大小选项，确定要返回的搜索匹配数。 默认为10。
        sourceBuilder.size(size);
        // 设置最小评分
        sourceBuilder.minScore(minScore);
        // 返回版本号
        sourceBuilder.version(true);
        // 设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        if (highlightFields != null) {
            for (String field : highlightFields) {
                highlightBuilder.field(field);
            }
        }
        sourceBuilder.highlighter(highlightBuilder);
        sourceBuilder.timeout(new TimeValue(timeout, TimeUnit.MILLISECONDS));
        sourceBuilder.sort("_score", SortOrder.ASC);

        log.info("search source builder: " + sourceBuilder);
        return sourceBuilder;
    }

    /**
     * @param aggregationBuilder 设置聚合对象
     * @param from               设置from选项，确定要开始搜索的结果索引。 默认为0。
     * @param size               设置大小选项，确定要返回的搜索匹配数。 默认为10。
     * @param timeout
     * @return org.elasticsearch.search.builder.SearchSourceBuilder
     * @throws
     * @date 2019/10/26 0:01
     * @since
     */
    public static SearchSourceBuilder initAggregationSourceBuilder(AggregationBuilder aggregationBuilder, int from, int size, int timeout,
                                                                   float minScore) {
        return initAggregationSourceBuilder(aggregationBuilder, from, size, timeout, minScore, Arrays.asList("demography_xm", "zyks", "zzys"));
    }
}
