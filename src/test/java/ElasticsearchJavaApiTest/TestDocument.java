package ElasticsearchJavaApiTest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.jupiter.api.*;

/*
    RFE:
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter8/TestDocument.java
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestDocument {

    private static  RestHighLevelClient restClient = null;
    private static RequestOptions REQUEST_OPTIONS_DEFAULT = null;

    @BeforeAll
    static void beforeAll() throws IOException {
        restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        REQUEST_OPTIONS_DEFAULT = RequestOptions.DEFAULT;
        deleteTwitterIndex();
        createTwitterIndex();
    }

    public static void deleteTwitterIndex() throws IOException{
        DeleteIndexRequest request = new DeleteIndexRequest("twitter");
        AcknowledgedResponse deleteResp = restClient.indices().delete(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(deleteResp.isAcknowledged());
    }

    private static void createTwitterIndex() throws IOException {
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
    /**
     * 索引文档
     */
    @Test
    @Order(1)
    public void createDoc() throws IOException {
        IndexRequest request = new IndexRequest("twitter");
        // json 字符串
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2019-08-29\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.id("1").source(jsonString, XContentType.JSON);
        IndexResponse jsonResp = restClient.index(request, REQUEST_OPTIONS_DEFAULT);
        Assertions.assertEquals("twitter", jsonResp.getIndex());
        Assertions.assertEquals("1", jsonResp.getId());
        System.out.println("index:" + jsonResp.getIndex());
        System.out.println("id: " + jsonResp.getId());
        if(jsonResp.getResult() == DocWriteResponse.Result.CREATED){
            // 新建文档
            System.out.println("doc CREATED");
        }else if(jsonResp.getResult() == DocWriteResponse.Result.UPDATED){
            // 更新文档
            System.out.println("doc UPDATED");
        }
        ReplicationResponse.ShardInfo shardInfo = jsonResp.getShardInfo();
        Assertions.assertEquals(3, shardInfo.getTotal());
        Assertions.assertEquals(1, shardInfo.getSuccessful());
        System.out.println("total shard: " + shardInfo.getTotal() + " success shard: " + shardInfo.getSuccessful());

        // map
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "map");
        jsonMap.put("postDate", "2019-08-29");
        jsonMap.put("message", "index map ES");
        request.id("2").source(jsonMap);
        IndexResponse mapResp = restClient.index(request, REQUEST_OPTIONS_DEFAULT);
        Assertions.assertEquals("2", mapResp.getId());
        Assertions.assertEquals("CREATED", mapResp.getResult().toString());

        System.out.println("index map, id:" + mapResp.getId() + " result:" + mapResp.getResult());

        // XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "xcontent");
            builder.timeField("postDate", "2019-08-29");
            builder.field("message", "index xcontent ES");
        }
        builder.endObject();
        request.id("3").source(builder);
        IndexResponse xcontentResp = restClient.index(request, REQUEST_OPTIONS_DEFAULT);
        Assertions.assertEquals("3", xcontentResp.getId());
        Assertions.assertEquals("CREATED", xcontentResp.getResult().toString());
        System.out.println("index xcontent, id:" + xcontentResp.getId() + " result:" + xcontentResp.getResult());

        // key-value
        request.id("4").source("user", "key-value", "postDate", "2019-08-29", "message", "index key-value ES");
        IndexResponse kvResp = restClient.index(request, REQUEST_OPTIONS_DEFAULT);
        Assertions.assertEquals("4", kvResp.getId());
        Assertions.assertEquals("CREATED", kvResp.getResult().toString());
        System.out.println("index key-value, id:" + kvResp.getId() + " result:" + kvResp.getResult());
    }




    /**
     * 获取文档
     */
    @Test
    @Order(2)
    public void getDoc() throws IOException{
        GetRequest getRequest = new GetRequest("twitter", "1");
        GetResponse getResponse = restClient.get(getRequest, REQUEST_OPTIONS_DEFAULT);
        Assertions.assertEquals("{\"user\":\"kimchy\",\"postDate\":\"2019-08-29\",\"message\":\"trying out Elasticsearch\"}", getResponse.getSourceAsString());
        Assertions.assertEquals(1l, getResponse.getVersion());
        System.out.println("getResponse.getSourceAsString():" + getResponse.getSourceAsString());
        System.out.println("getResponse.getVersion():" + getResponse.getVersion());
    }


    /**
     * 批量获取
     */
    @Test
    @Order(3)
    public void multiGet() throws IOException{
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("twitter", "3"));
        request.add("books", "2");
        MultiGetResponse mget = restClient.mget(request, REQUEST_OPTIONS_DEFAULT);
        MultiGetItemResponse[] responses = mget.getResponses();
        for(MultiGetItemResponse resp: responses){
            Assertions.assertFalse(resp.isFailed());
            System.out.println(resp.getIndex() + " : " + resp.getId());
            GetResponse response = resp.getResponse();
            System.out.println(response.getVersion());
            System.out.println(response.getSourceAsString());
            System.out.println("-------------------");
        }
    }
    /**
     * 修改文档
     */
    @Test
    @Order(4)
    public void updateDoc() throws IOException{
        UpdateRequest request = new UpdateRequest("twitter", "1");
        /**
         * 脚本方式
         */
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("count", 4);
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.count = params.count", parameters);
        request.script(inline);
        UpdateResponse update = restClient.update(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(update.getId() + " : " + update.getVersion());

        /**
         * upsert 文档存在，则更新；文档不存在，则新增
         */
        UpdateRequest request2 = new UpdateRequest("twitter", "5").doc("updateTime", new Date(), "reason", "daily update").upsert("create", new Date());
        UpdateResponse update2 = restClient.update(request2, REQUEST_OPTIONS_DEFAULT);
        System.out.println(update2.getId() + " : " + update2.getVersion());
    }

    /**
     * 删除文档
     */
    @Test
    @Order(5)
    public void delDoc() throws IOException{
        DeleteRequest request = new DeleteRequest("twitter", "1");
        DeleteResponse resp = restClient.delete(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(resp.getIndex());
        System.out.println(resp.getId());
        ReplicationResponse.ShardInfo shardInfo = resp.getShardInfo();
        System.out.println(shardInfo.getTotal() + " - " + shardInfo.getSuccessful());
    }



    /**
     * 查询删除
     */
    @Test
    @Order(6)
    public void deleteByQuery() throws IOException{
        DeleteByQueryRequest request = new DeleteByQueryRequest("twitter");
        request.setQuery(new TermQueryBuilder("_id", 5));
        BulkByScrollResponse deleteByQuery = restClient.deleteByQuery(request, REQUEST_OPTIONS_DEFAULT);
        System.out.println(deleteByQuery.getTotal() + " : " + deleteByQuery.getDeleted());
    }



    /**
     * 批量操作
     */
    @Test
    @Order(7)
    public void bulk() throws IOException{
        BulkRequest request = new BulkRequest();

        /**
         * positive case
         */
        request.add(new IndexRequest("twitter").id("6").source(XContentType.JSON, "user", "bulk", "message", "add by bulk id 6", "postDate", new Date()));

        /**
         * failure case
         */
        UpdateRequest four = null;
        {
            four = new UpdateRequest("twitter", "1");
            // 脚本方式
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("count", 4);
            Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.count = params.count", parameters);
            four.script(inline);
        }
        request.add(four);

        /**
         * positive case
         */
        request.add(new IndexRequest("twitter").id("7").source("user", "bulk", "message", "add by bulk id 7", "postDate", new Date()));
        request.add(new IndexRequest("test").id("bulk").source("foo", "hello bulk"));


        BulkResponse bulkResponse = restClient.bulk(request, REQUEST_OPTIONS_DEFAULT);
        for(BulkItemResponse response: bulkResponse){
            boolean isFailed = response.isFailed();
            if(isFailed) {
                /*
                    failed at 第1个input
                 */
                System.out.println(String.format("failed at 第%s个input", response.getItemId()));
                /*
                    failed response.getIndex(): twitter, failed response.getId(): 1, failed response.getFailureMessage(): [twitter/dwVlV8pBRfWs_xD0qFAQJQ][[twitter][2]] ElasticsearchException[Elasticsearch exception [type=document_missing_exception, reason=[_doc][1]: document missing]]
                 */
                System.out.println(String.format("failed response.getIndex(): %s, failed response.getId(): %s, failed response.getFailureMessage(): %s", response.getIndex(), response.getId(), response.getFailureMessage()));
                continue;
            }

            DocWriteResponse resp = response.getResponse();

            switch(response.getOpType()){
                case INDEX:
                    System.out.println(resp.getIndex() + " - " + resp.getId() + " - INDEX");
                case CREATE:
                    System.out.println(resp.getIndex() + " - " + resp.getId() + " - CREATE");
                    IndexResponse indexResponse = (IndexResponse) resp;
                    System.out.println(String.format("indexResponse.getResult(): %s", indexResponse.getResult()));
                    break;
                case UPDATE:
                    System.out.println(resp.getIndex() + " - " + resp.getId() + " - UPDATE");
                    UpdateResponse updateResponse = (UpdateResponse) resp;
                    System.out.println(String.format("updateResponse.getResult(): %s", updateResponse.getResult()));
                    break;
                case DELETE:
                    System.out.println(resp.getIndex() + " - " + resp.getId() + " - DELETE");
                    DeleteResponse deleteResponse = (DeleteResponse) resp;
                    System.out.println(String.format("deleteResponse.getResult(): %s", deleteResponse.getResult()));
                    break;
            }
            System.out.println("----------------------------------");
        }
    }

    /**
     * bulk 批量操作支持在操作完成之前和之后进行相应的操作
     */
    @Test
    @Order(8)
    public void bulkProcessor() throws InterruptedException{
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // 批量操作提交之前执行
                Gson gson = new Gson();
                System.out.println("bulkProcessor.beforeBulk: " + executionId + " - " + request.requests());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                // 批量操作提交之后执行
                System.out.println("after: " + executionId + " - " + request.numberOfActions());
                System.out.println(String.format("bulkProcessor.afterBulk: response.hasFailures() = %s", response.hasFailures()));
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                // 批量操作异常时执行
                System.out.println("bulkProcessor.afterBulk: failure = " + executionId + " - " + request.toString());
            }
        };
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> restClient.bulkAsync(request, REQUEST_OPTIONS_DEFAULT, bulkListener),
                listener)
                .setBulkActions(1)  // 请求数量多少次提交
                .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))    // 批处理请求达到20M 时提交
                .setFlushInterval(TimeValue.timeValueSeconds(10))    // 设置刷新索引的时间间隔
                .setConcurrentRequests(5)   // 设置并发处理线程个数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))  // 设置回滚策略，等待时间100ms，重试次数3次
                .build();

        /**
         * positive case
         */
        IndexRequest one = new IndexRequest("twitter").id("9").source(XContentType.JSON, "user", "bulk", "message", "add by bulkProcessor id 9", "postDate", new Date());
        IndexRequest two = new IndexRequest("twitter").id("8").source("user", "bulk", "message", "add by bulkProcessor id 8", "postDate", new Date());
        IndexRequest three = new IndexRequest("test").id("bulkProcessor").source("foo", "hello bulkProcessor");

        /**
         * failure case
         */
        UpdateRequest four = null;
        {
            four = new UpdateRequest("twitter", "1");
            // 脚本方式
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("count", 4);
            Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.count = params.count", parameters);
            four.script(inline);
        }

        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);
        bulkProcessor.add(four);

        boolean isBulkProcessorSucceed = bulkProcessor.awaitClose(60, TimeUnit.SECONDS);
        // true 所有批量请求都已完成；false 所有批量请求完成之前等待的时间已过
        System.out.println(String.format("isBulkProcessorSucceed: %s", isBulkProcessorSucceed) );
    }

}