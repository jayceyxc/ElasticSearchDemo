package com.linus.es.demo.utils;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Stack;

/**
 * @author yuxuecheng
 * @Title: RegexUtil
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/21 14:14
 */
public class RegexUtil {

    public static void main(String[] args) {
        ElasticQueryBuilderUtil elasticQueryBuilderUtil = new ElasticQueryBuilderUtil();
        String allFileds = "admission_dep,admission_ward,attending_physician";
        String text = "(达比加群|华法林)&肌钙蛋白";
        Stack<String> words = new Stack<>();
        Stack<Character> operators = new Stack<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (char ch : text.toCharArray()) {
            if (ch == '&' || ch == '|') {
                if (words.isEmpty()) {
                    System.err.println("表达式错误：" + text);
                    return;
                }
                operators.push(ch);
            } else if (ch == '!') {
                if (words.isEmpty()) {
                    System.err.println("表达式错误：" + text);
                    return;
                }
                String word = words.pop();
                BoolQueryBuilder subQueryBuilder = elasticQueryBuilderUtil.buildAllQueryBuilder(word, allFileds);
                boolQueryBuilder.mustNot(subQueryBuilder);
            }
        }
    }
}
