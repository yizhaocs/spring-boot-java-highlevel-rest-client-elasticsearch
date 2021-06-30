package ElasticsearchJavaApiTest;

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
import org.junit.jupiter.api.Test;

/*
    RFE:
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter8/TestClient.java
 */
/*
 *跑这个测试前，现在Kibana上创建books index，并且插入些数据：
 *
 curl -XDELETE http://localhost:9200/books/
 curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/_bulk?pretty" --data-binary @/Users/yzhao/Documents/code/spring-boot-java-highlevel-rest-client-elasticsearch/src/test/resources/books.json
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