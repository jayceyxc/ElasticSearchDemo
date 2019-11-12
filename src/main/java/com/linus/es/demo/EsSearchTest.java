package com.linus.es.demo;

import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.utils.ElasticUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

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

    @Autowired
    BaseElasticDao elasticDao;

    private String nestedDiagnosisDataSearch() {
        BoolQueryBuilder nestedBoolQueryBuilder = QueryBuilders.boolQuery();
        MatchQueryBuilder diagnosisNameMatchQuery = QueryBuilders.matchQuery("diagnosis_data.diagnosis_name", "冠状动脉粥样硬化性心脏病");
        nestedBoolQueryBuilder.must(diagnosisNameMatchQuery);
        NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery("diagnosis_data", nestedBoolQueryBuilder, ScoreMode.Max);

        MatchQueryBuilder chiefComplaintMatchQuery = QueryBuilders.matchQuery("chief_complaint", "胸闷");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(nestedQueryBuilder);
        boolQueryBuilder.must(chiefComplaintMatchQuery);
        SearchSourceBuilder searchSourceBuilder = ElasticUtil.initSearchSourceBuilder(boolQueryBuilder, 0, 100, 2000, 0.5f);
        String searchResult = elasticDao.search("original", searchSourceBuilder);
        log.info(searchResult);
        return searchResult;
    }

    @Override
    public void run(String... args) throws Exception {
        String result = nestedDiagnosisDataSearch();
        log.info(result);
    }
}
