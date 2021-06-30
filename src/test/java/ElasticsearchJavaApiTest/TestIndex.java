package ElasticsearchJavaApiTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/*
    RFE:
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter8/TestIndex.java
 */
/*
 *跑这个测试前，现在Kibana上创建books index，并且插入些数据：
 *
 curl -XDELETE http://localhost:9200/books/
 curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/_bulk?pretty" --data-binary @/Users/yzhao/Documents/code/spring-boot-java-highlevel-rest-client-elasticsearch/src/test/resources/books.json
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestIndex {

    private RestHighLevelClient restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    private RequestOptions REQUEST_OPTIONS_DEFAULT = RequestOptions.DEFAULT;



    /**
     * 删除索引，也支持异步
     */
    @Test
    @Order(1)
    public void deleteIndex() throws IOException{
        DeleteIndexRequest request = new DeleteIndexRequest("twitter");
        AcknowledgedResponse deleteResp = restClient.indices().delete(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(deleteResp.isAcknowledged());
    }

    /**
     * 判断索引是否存在
     */
    @Test
    @Order(2)
    public void existsIndex() throws IOException{
        IndicesClient indices = restClient.indices();
        GetIndexRequest request = new GetIndexRequest("books");
        boolean exists = indices.exists(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(exists);
    }

    /**
     * 异步方式判断索引是否存在
     */
    @Test
    @Order(3)
    public void existsIndexAsync(){
        IndicesClient indices = restClient.indices();
        GetIndexRequest request = new GetIndexRequest("books");
        indices.existsAsync(request, REQUEST_OPTIONS_DEFAULT,
                new ActionListener<Boolean>() {
                    @Override
                    public void onResponse(Boolean result) {
                        System.out.println(result);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        System.out.println("end");
    }

    /**
     * 创建索引，也支持异步
     */
    @Test
    @Order(4)
    public void createIndex() throws IOException{
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        // 设置settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );

        // 设置mapping
        request.mapping(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"keyword\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}", XContentType.JSON);
        // 还支持map 方式
        Map<String, Object> message = new HashMap<>();
        message.put("type", "keyword");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
//        request.mapping(mapping);
        // 还支持XContentBuilder
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
                {
                    builder.field("type", "keyword");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
//        request.mapping(builder);

        // 设置别名
        request.alias(new Alias("tw_alias"));

        CreateIndexResponse createResp = restClient.indices().create(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(createResp.isAcknowledged());
        System.out.println(createResp.index());
    }

/*

    */
/**
     * 获取索引的mappings
     *//*

    @Test
    @Order(5)
    public void getIndexMapping() throws IOException{
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("twitter");
        GetMappingsResponse mappingResp = restClient.indices().getMapping(request, REQUEST_OPTIONS_DEFAULT);

        Map<String, MappingMetaData> allMappings = mappingResp.mappings();
        MappingMetaData mappingMetaData = allMappings.get("twitter");
        Map<String, Object> map = mappingMetaData.sourceAsMap();
        System.out.println(allMappings.size());
        System.out.println(JSON.toJSONString(map, true));
    }
*/

    /**
     * 刷新索引
     */
    @Test
    @Order(6)
    public void refreshIndex() throws IOException{
        RefreshRequest request = new RefreshRequest("twitter");
        RefreshResponse refreshResp = restClient.indices().refresh(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(refreshResp.getTotalShards());
        System.out.println(refreshResp.getSuccessfulShards());
        System.out.println(refreshResp.getFailedShards());
    }
/*
    *//**
     * 关闭索引，打开索引
     *//*
    @Test
    @Order(7)
    public void closeAndOpenIndex() throws IOException{
        CloseIndexRequest closeReq = new CloseIndexRequest("twitter");
        AcknowledgedResponse close = restClient.indices().close(closeReq, REQUEST_OPTIONS_DEFAULT);
        System.out.println("close: " + close.isAcknowledged());

        OpenIndexRequest openReq = new OpenIndexRequest("twitter");
        OpenIndexResponse open = restClient.indices().open(openReq, REQUEST_OPTIONS_DEFAULT);
        System.out.println("open: " + open.isAcknowledged());
        System.out.println(open.isShardsAcknowledged());
    }
    */
/*

    */
/**
     * 获取别名，设置别名
     * @throws IOException
     *//*

    @Test
    @Order(8)
    public void aliasIndex() throws IOException{
        // 根据别名获取索引
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest("tw_alias");
        GetAliasesResponse getAliasResp = restClient.indices().getAlias(getAliasesRequest, REQUEST_OPTIONS_DEFAULT);
        Map<String, Set<AliasMetaData>> aliases = getAliasResp.getAliases();
        System.out.println(JSON.toJSONString(aliases));

        // 增加别名
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("twitter").alias("alias2");
        aliasesRequest.addAliasAction(addAction);
        AcknowledgedResponse addAliases = restClient.indices().updateAliases(aliasesRequest, REQUEST_OPTIONS_DEFAULT);
        System.out.println("add aliase: " + addAliases.isAcknowledged());

        // 删除别名
        AliasActions delAction = new AliasActions(AliasActions.Type.REMOVE).index("twitter").alias("alias2");
        aliasesRequest.addAliasAction(delAction);
        AcknowledgedResponse delAliases = restClient.indices().updateAliases(aliasesRequest, REQUEST_OPTIONS_DEFAULT);
        System.out.println("delete alias: " + delAliases.isAcknowledged());
    }
*/

}