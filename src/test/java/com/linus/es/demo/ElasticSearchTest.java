package com.linus.es.demo;

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.locationtech.jts.geom.Coordinate;

/**
 * @author yuxuecheng
 * @Title: ElasticSearchTest
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/8 11:29
 */
public class ElasticSearchTest {
    public static void main(String[] args) {
        Coordinate topLeft = new Coordinate(106.23248, 38.48644);
        Coordinate bottomRight = new Coordinate(115.85794, 28.68202);
        QueryBuilder geoShapeQuery = null;
    }
}
