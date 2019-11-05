package com.linus.es.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

    /**
     * 单位ms
     */
    @Value("${es.username}")
    public String userName;

    /**
     * 单位ms
     */
    @Value("${es.password}")
    public String password;

    @Bean
    public RestClientBuilder restClientBuilder() {
        // 用户名密码授权 https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/_basic_authentication.html
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));

        log.info("配置信息：socket timeout: {}, connect timeout: {}, user name: {}, password: {}",
                socketTimeout, connectTimeout, userName, password);
        RestClientBuilder builder = RestClient.builder(makeHttpHost());
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            requestConfigBuilder.setConnectTimeout(connectTimeout);

            return requestConfigBuilder;
        });
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36");
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });

        return builder;
    }

    private HttpHost makeHttpHost() {
        return new HttpHost(host, port, scheme);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        return new RestHighLevelClient(restClientBuilder);
    }
}
