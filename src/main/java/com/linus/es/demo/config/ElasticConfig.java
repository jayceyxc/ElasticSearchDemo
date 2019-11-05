package com.linus.es.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author yuxuecheng
 * @Title: ElasticConfig
 * @ProjectName demo
 * @Description: TODO
 * @date 2019-11-02 15:56
 */
@Slf4j
@Configuration
public class ElasticConfig {

    @Value("${es.host}")
    public String host;

    @Value("${es.port}")
    public int port;

    @Value("${es.scheme}")
    public String scheme;

    /**
     * 单位ms
     */
    @Value("${es.socket_timeout}")
    public int socketTimeout;

    /**
     * 单位ms
     */
    @Value("${es.connect_timeout}")
    public int connectTimeout;

    @Bean
    public RestClientBuilder restClientBuilder() {
        log.info("配置信息：socket timeout: {}, connect timeout: {}", socketTimeout, connectTimeout);
        RestClientBuilder builder = RestClient.builder(makeHttpHost());
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            requestConfigBuilder.setConnectTimeout(connectTimeout);

            return requestConfigBuilder;
        });
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36"));
        return builder;
    }

    private HttpHost makeHttpHost() {
        return new HttpHost(host, port, scheme);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Autowired RestClientBuilder restClientBuilder){
        return new RestHighLevelClient(restClientBuilder);
    }
}
