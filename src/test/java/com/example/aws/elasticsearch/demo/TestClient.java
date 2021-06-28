package com.example.aws.elasticsearch.demo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Test;

/*
    RFE:
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter8/TestClient.java
 */
@SuppressWarnings({ "deprecation", "resource" })
public class TestClient {

    private static String CLUSTER_NAME = "elasticsearch";   // 集群名称
    private static String HOST_IP = "localhost";   // 服务器IP
    private static int TCP_PORT = 9300;     // 端口

    /**
     * 7.2 RestHighLevelClient 客户端
     */
    @Test
    public void testRestClient() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST_IP, 9200, "http")));
        GetRequest getRequest = new GetRequest("books", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }
}