package com.linus.es.demo.controller;

import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.utils.BoolExpressionParser;
import com.linus.es.demo.utils.ElasticQueryBuilderUtil;
import com.linus.es.demo.utils.ElasticUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * @author yuxuecheng
 * @Title: ElasticWebController
 * @ProjectName demo
 * @Description: 返回web页面的视图格式
 * @date 2019/12/25 10:58
 */
@Slf4j
@RequestMapping("/web/")
@RestController
public class ElasticWebController {

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

    /**
     * ElasticSearch搜索时使用的分词器
     */
    @Value("${es.search_analyzer}")
    public String searchAnalyzer;

    /**
     * ElasticSearch全字段搜索时使用的字段名列表
     */
    @Value("${es.all_fields}")
    public String allFields;

    @Autowired
    private BaseElasticDao baseElasticDao;

    /**
     * bool查询入口。
     * 支持&（AND）、|（OR）和!（NOT）
     * 运算符优先级，从高到低依次为!、&、|，可以使用括号来改变运算词序
     * 例如：
     * 华法林&肌钙蛋白
     * 达比加群!华法林
     * (达比加群|华法林)&肌钙蛋白
     * @param index 搜索的索引名
     * @param text 布尔搜索文本
     * @return 搜索结果
     */
    @RequestMapping(value = "/bool_search/{index}/{text}", method = RequestMethod.GET)
    public ModelAndView boolSearch(@PathVariable("index") String index, @PathVariable("text") String text){
        log.info("enter boolSearch");
        ModelAndView modelAndView = new ModelAndView();
        Instant start = Instant.now();
        ElasticQueryBuilderUtil queryBuilderTools = new ElasticQueryBuilderUtil(searchAnalyzer);
        Queue<String> formalizedText = BoolExpressionParser.convert(text);
        log.info("输入搜索字符串：" + text + "，格式化后字符串：" + formalizedText);
        BoolQueryBuilder boolQueryBuilder = queryBuilderTools.buildBoolQueryBuilder(formalizedText, allFields);

        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(boolQueryBuilder, null, 0, 100, 2000, 0.5f,
                Arrays.asList("admission_dep", "chief_complaint", "first_disease_course", "personal_history", "present_illness_history"));
        SearchResponse searchResponse = baseElasticDao.searchReturnRaw(index, searchSourceBuilder);

        Instant end = Instant.now();
        List<Map<Object, Object>> result = new ArrayList<>();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        for (SearchHit hit : searchHits) {
            Map<Object, Object> map = new LinkedHashMap<>();
            map.put("Source As String", hit.getSourceAsString());
            // 返回String格式的文档结果
            System.out.println("Source As String:" + hit.getSourceAsString());
            map.put("Source As Map", hit.getSourceAsMap());
            // 返回Map格式的文档结果
            System.out.println("Source As Map:" + hit.getSourceAsMap());
            // 返回文档所在的索引
            map.put("Index", hit.getIndex());
            System.out.println("Index:" + hit.getIndex());
            // 返回文档所在的ID编号
            map.put("Id", hit.getId());
            System.out.println("Id:" + hit.getId());
            // 返回指定字段的内容,例如这里返回完整的title的内容
            map.put("Name", hit.getSourceAsMap().get("name"));
            System.out.println("name: " + hit.getSourceAsMap().get("name"));
            // 返回文档的评分
            map.put("Score", hit.getScore());
            System.out.println("Score:" + hit.getScore());
            // 返回文档的高亮字段
            Map<String, HighlightField> highlightFieldMap = hit.getHighlightFields();
            StringBuilder hight = new StringBuilder();
            for (Map.Entry<String, HighlightField> entry : highlightFieldMap.entrySet()) {
                Text[] textValue = entry.getValue().getFragments();
                if (textValue != null) {
                    for (Text str : textValue) {
                        hight.append(str);
                        System.out.println(str.toString());
                    }
                    hight.append("\r\n\r\n");
                }
            }
            System.out.println(hight.toString());
            map.put("Highlight", hight.toString());
            result.add(map);
        }

        modelAndView.addObject("resultlist", result);
        modelAndView.addObject("count", "检索出: "+"<span style=\"color:red;font-weight:bold;font-size:18px;\">"+searchResponse.getHits().getTotalHits().value+"</span>"+"条记录");
        modelAndView.addObject("time", ",共耗时: "+"<span style=\"color:red;font-weight:bold;font-size:18px;\">"+ Duration.between(start, end).toMillis() + "</span>"+ "ms");
        modelAndView.setViewName("result");
        return modelAndView;
    }
}
