package ElasticsearchJavaApiTest;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DocValueFormat.DateTime;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;


/*
    RFE:
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter8/TestQuery.java
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter6/README.md
    https://github.com/windfish/essay/blob/a259ee0f05dbb33ecba57c8b71c57eee41302f77/src/com/demon/lucene/book/chapter6/books
 */

/*
 *跑这个测试前，现在Kibana上创建books index，并且插入些数据：
 *
 Kibana Query如下:
    DELETE books

    PUT books?include_type_name=true
    {
      "settings": {
        "number_of_replicas": 1,
        "number_of_shards": 3
      },
      "mappings": {
        "IT": {
          "properties": {
            "id": {
              "type": "long"
            },
            "title": {
              "type": "text"
            },
            "language": {
              "type": "keyword"
            },
            "author": {
              "type": "keyword"
            },
            "price": {
              "type": "double"
            },
            "publish time": {
              "type": "date",
              "format": "yyyy-mm-dd"
            },
            "description": {
              "type": "text"
            }
          }
        }
      }
    }
 POST /books/_doc
{
  "id": "1",
  "title": "java编程思想",
  "language": "java",
  "author": "Bruce Eckel",
  "price": 70.2,
  "publish time": "2007-10-01",
  "description": "Java学习必读经典,殿堂级著作!赢得了全球程序员的广泛赞誉。"
}

POST /books/_doc
{
  "id": "2",
  "title": "Java程序性能优化",
  "language": "java",
  "author": "葛一鸣",
  "price": 46.5,
  "publish time": "2012-08-01",
  "description": "让你的java程序更快、更稳定。深入剖析软件设计层面、代码层面、JVM虚拟机层面的优化方法"
}

POST /books/_doc
{
  "id": "3",
  "title": "Python科学计算",
  "language": "python",
  "author": "张若愚",
  "price": 81.4,
  "publish time": "2016-05-01",
  "description": "零基础学python,光盘中作者独家整合开发 winPython运行环境,涵盖了 Python各个扩展库"
}


POST /books/_doc
{
  "id": "4",
  "title": "Python基础教程",
  "language": "python",
  "author": "Helant",
  "price": 54.5,
  "publish time": "2014-03-01",
  "description": "经典的 Python入门教程,层次鲜明,结构严谨,内容翔实"
}

POST /books/_doc
{
  "id": "5",
  "title": "JavaScript高级程序设计",
  "language": "javascript",
  "author": "Nicholas C. Zakas ",
  "price": 66.4,
  "publish time": "2012-10-01",
  "description": "JavaScript技术经典名著"
}

 *
 */
/*
DELETE geo

PUT geo?include_type_name=true
{
  "mappings": {
    "city": {
      "properties": {
        "name":{
          "type": "keyword"
        },
        "location": {
          "type": "geo_point"
        }
      }
    }
  }
}

curl -H "Content-Type: application/json" -XPOST "http://localhost:9200/_bulk?pretty" --data-binary @/Users/yzhao/Documents/code/spring-boot-java-highlevel-rest-client-elasticsearch/src/test/resources/geo.json
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestQuery {

    private static  RestHighLevelClient restClient = null;
    private static RequestOptions REQUEST_OPTIONS_DEFAULT = null;

    @BeforeAll
    static void beforeAll() throws IOException {
        restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        REQUEST_OPTIONS_DEFAULT = RequestOptions.DEFAULT;
    }
    @Test
    @Order(1)
    public void matchQuery() throws IOException{
        SearchRequest searchRequest = new SearchRequest("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();    // 查询的参数都可以放入 SearchSourceBuilder 中
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());   // 全部匹配
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC)); // 按分数倒序排序，默认排序方式
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC)); // 支持排序，按id 升序排序
        searchRequest.source(searchSourceBuilder);
        SearchResponse allSearch = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        SearchHits hits = allSearch.getHits();
        hits.forEach((hit) -> {
            /*
            hit.getScore(): 1.0
            hit.getSourceAsString(): {"id":"1","title":"java编程思想","language":"java","author":"Bruce Eckel","price":70.2,"publish time":"2007-10-01","description":"Java学习必读经典,殿堂级著作!赢得了全球程序员的广泛赞誉。"}
            hit.getSourceAsMap(): {author=Bruce Eckel, price=70.2, description=Java学习必读经典,殿堂级著作!赢得了全球程序员的广泛赞誉。, language=java, id=1, title=java编程思想, publish time=2007-10-01}
             */
            System.out.println(String.format("hit.getScore(): %s", hit.getScore()) );
            System.out.println(String.format("hit.getSourceAsString(): %s", hit.getSourceAsString()));
            System.out.println(String.format("hit.getSourceAsMap(): %s", hit.getSourceAsMap()));
        });
        System.out.println("--------------------------------");

        searchSourceBuilder.query(QueryBuilders.termQuery("language", "java"));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(60));
        searchRequest.source(searchSourceBuilder);
        SearchResponse termSearch = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        termSearch.getHits().forEach((hit) -> {
            System.out.println(hit.getScore() + " - " + hit.getSourceAsString());
        });
        System.out.println("--------------------------------");

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("language", "python"); // 全文匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO); // 启用模糊匹配
        matchQueryBuilder.prefixLength(3); // 设置前缀长度选项
        matchQueryBuilder.maxExpansions(10); // 设置最大扩展选项，以控制查询的模糊过程
        searchSourceBuilder.query(matchQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse queryBuilderResp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        queryBuilderResp.getHits().forEach((hit) -> {
            System.out.println(hit.getScore() + " - " + hit.getSourceAsString());
        });
        System.out.println("--------------------------------");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title"); // 高亮字段
        highlightTitle.highlighterType("unified"); // 高亮类型
        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);
        searchSourceBuilder.highlighter(highlightBuilder);
        MatchQueryBuilder highlightMatchQueryBuilder = new MatchQueryBuilder("title", "python"); // 全文匹配
        searchSourceBuilder.query(highlightMatchQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse highlightQueryResp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        highlightQueryResp.getHits().forEach((hit) -> {
            System.out.println(hit.getScore() + " - " + hit.getSourceAsString());
            Text[] text = hit.getHighlightFields().get("title").fragments();
            System.out.println(text[0].toString());
        });
        System.out.println("--------------------------------");

        TermSuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("title").text("程序");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_title", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse suggestResp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Suggest suggest = suggestResp.getSuggest();
        TermSuggestion termSuggestion = suggest.getSuggestion("suggest_title");
        Gson gson = new Gson();
        System.out.println(gson.toJson(termSuggestion));
        for(TermSuggestion.Entry entry: termSuggestion.getEntries()){
            for(TermSuggestion.Entry.Option option: entry){
                String suggestText = option.getText().string();
                System.out.println(suggestText);
            }
        }
        System.out.println("--------------------------------");

        searchRequest = new SearchRequest("books");
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());   // 全部匹配
        // 对书籍创建language 的术语聚合，并对其平均价格做子聚合
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("by_language").field("language");
        aggregationBuilder.subAggregation(AggregationBuilders.avg("average_price").field("price"));
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse aggregationResp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Aggregations aggregations = aggregationResp.getAggregations();
        Terms byLanguageAggregation = aggregations.get("by_language");
        for(Bucket bucket: byLanguageAggregation.getBuckets()){
            Avg avgPrice = bucket.getAggregations().get("average_price");
            double avg = avgPrice.getValue();
            System.out.println(bucket.getKeyAsString() + " price avg: " + avg);
        }
    }

    @SuppressWarnings("unused")
    public void fullTextQuery(){
        MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery(); // 全部匹配

        MatchPhraseQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery("title", "java"); // 匹配短语

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQuery = QueryBuilders.matchPhrasePrefixQuery("title", "ja"); // 匹配词前缀

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("java", "title", "description"); // 多字段匹配

        CommonTermsQueryBuilder commonTermsQuery = QueryBuilders.commonTermsQuery("title", "java"); // 通用词项匹配

        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery("+kimchy -elasticsearch");

        SimpleQueryStringBuilder simpleQueryStringQuery = QueryBuilders.simpleQueryStringQuery("+kimchy -elasticsearch");
    }

    @SuppressWarnings("unused")
    public void termQuery(){
        QueryBuilder termQuery = QueryBuilders.termQuery("title", "java"); // 词项查询

        QueryBuilder termsQuery = QueryBuilders.termsQuery("title", "java", "python"); // 多词项查询

        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").from(50).to(70).includeLower(true).includeUpper(false);

        QueryBuilder existsQuery = QueryBuilders.existsQuery("language");

        QueryBuilder prefixQuery = QueryBuilders.prefixQuery("description", "win");

        QueryBuilder wildcardQuery = QueryBuilders.wildcardQuery("author", "张若?"); // 通配符查询

        QueryBuilder regexpQuery = QueryBuilders.regexpQuery("author", "Br.*"); // 正则表达式

        QueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("title", "java"); // 模糊查询

        QueryBuilder idsQuery = QueryBuilders.idsQuery().addIds("3", "5");
    }

    @SuppressWarnings("unused")
    public void compoundQuery(){
        QueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("title", "java")).boost(2.0f); // 固定分数查询

        QueryBuilder disMaxQuery = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("title", "java"))
                .add(QueryBuilders.termQuery("title", "python"))
                .boost(1.2f)
                .tieBreaker(0.7f);

        // bool query 查询title 字段包含java，价格不高于70，description 字段可以包含也可以不包含“虚拟机”的书籍
        QueryBuilder matchQuery1 = QueryBuilders.matchQuery("title", "java");
        QueryBuilder matchQuery2 = QueryBuilders.matchQuery("description", "虚拟机");
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").gte(70);
        QueryBuilder boolQuery = QueryBuilders.boolQuery().must(matchQuery1).should(matchQuery2).mustNot(rangeQuery);

        // boosting query 查询title 包含python 的书籍，并对出版日期在2015年之前的降低评分
        QueryBuilder matchQuery = QueryBuilders.matchQuery("title", "python");
        QueryBuilder rangeQuery2 = QueryBuilders.rangeQuery("publish_time").lte("2015-01-01");
        QueryBuilder boostingQuery = QueryBuilders.boostingQuery(matchQuery, rangeQuery2).negativeBoost(0.2f);
    }

    @SuppressWarnings("unused")
    public void nestingQuery() throws IOException{
        QueryBuilder qb = QueryBuilders.nestedQuery("obj1",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("obj1.name", "blue"))
                        .must(QueryBuilders.rangeQuery("obj1.count").gt(5)),
                ScoreMode.Avg);

    }

    /**
     * 指标聚合
     */
    @Test
    @Order(2)
    public void aggregation1() throws IOException{
        SearchRequest searchRequest = new SearchRequest("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("max_agg").field("price"); // 创建一个求最大值的聚合agg，聚合字段为price
        searchSourceBuilder.aggregation(maxAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Max max = resp.getAggregations().get("max_agg");
        System.out.println("max: " + max.getValue());

        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("min_agg").field("price");
        searchSourceBuilder.aggregation(minAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Min min = resp.getAggregations().get("min_agg");
        System.out.println("min: " + min.getValue());

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_agg").field("price");
        searchSourceBuilder.aggregation(sumAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Sum sum = resp.getAggregations().get("sum_agg");
        System.out.println("sum: " + sum.getValue());

        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_agg").field("price");
        searchSourceBuilder.aggregation(avgAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Avg avg = resp.getAggregations().get("avg_agg");
        System.out.println("avg: " + avg.getValue());

        StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats("stats_agg").field("price");
        searchSourceBuilder.aggregation(statsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Stats stats = resp.getAggregations().get("stats_agg");
        System.out.println("-------------stats----------------");
        System.out.println(stats.getMin());
        System.out.println(stats.getMax());
        System.out.println(stats.getAvg());
        System.out.println(stats.getSum());
        System.out.println(stats.getCount());
        System.out.println("-------------stats----------------");

        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders.extendedStats("ex_stats_agg").field("price");
        searchSourceBuilder.aggregation(extendedStatsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        ExtendedStats extendedStats = resp.getAggregations().get("ex_stats_agg");
        System.out.println("-------------extended stats----------------");
        System.out.println(extendedStats.getMin());
        System.out.println(extendedStats.getMax());
        System.out.println(extendedStats.getAvg());
        System.out.println(extendedStats.getSum());
        System.out.println(extendedStats.getStdDeviation());
        System.out.println(extendedStats.getSumOfSquares());
        System.out.println(extendedStats.getVariance());
        System.out.println("-------------extended stats----------------");

        CardinalityAggregationBuilder cardinalityAggregationBuilder = AggregationBuilders.cardinality("card_agg").field("language");
        searchSourceBuilder.aggregation(cardinalityAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Cardinality cardinality = resp.getAggregations().get("card_agg");
        System.out.println(cardinality.getValue());

        PercentilesAggregationBuilder percentilesAggregationBuilder = AggregationBuilders.percentiles("percentiles_agg").field("price");
        searchSourceBuilder.aggregation(percentilesAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Percentiles percentiles = resp.getAggregations().get("percentiles_agg");
        System.out.println("-------------percentiles----------------");
        for(Percentile entry: percentiles){
            double percent = entry.getPercent();
            double value = entry.getValue();
            System.out.println("percent: " + percent + ", value: " + value);
        }
        System.out.println("-------------percentiles----------------");

        ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders.count("value_count_agg").field("language");
        searchSourceBuilder.aggregation(valueCountAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        ValueCount valueCount = resp.getAggregations().get("value_count_agg");
        System.out.println("value count: " + valueCount.getValue());
    }

    /**
     * 桶聚合
     */
    @Test
    @Order(3)
    public void aggregation2() throws IOException{
        SearchRequest searchRequest = new SearchRequest("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("per_count").field("language");
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Terms terms = resp.getAggregations().get("per_count");
        System.out.println("-------------terms----------------");
        for(Terms.Bucket entry: terms.getBuckets()){
            System.out.println(entry.getKey() + " --- " + entry.getDocCount());
        }
        System.out.println("-------------terms----------------");

        FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders.filter("filter_agg", QueryBuilders.termQuery("title", "java"));
        searchSourceBuilder.aggregation(filterAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Filter filter = resp.getAggregations().get("filter_agg");
        System.out.println(filter.getDocCount());

        FiltersAggregationBuilder filtersAggregationBuilder = AggregationBuilders.filters("filters_agg",
                new FiltersAggregator.KeyedFilter("java", QueryBuilders.termQuery("title", "java")),
                new FiltersAggregator.KeyedFilter("python", QueryBuilders.termQuery("title", "python")));
        searchSourceBuilder.aggregation(filtersAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Filters filters = resp.getAggregations().get("filters_agg");
        System.out.println("-------------filters----------------");
        for(Filters.Bucket entry: filters.getBuckets()){
            String key = entry.getKeyAsString();
            System.out.println(key + " --- " + entry.getDocCount());
        }
        System.out.println("-------------filters----------------");

        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("range_agg").field("price")
                .addUnboundedTo(50).addRange(50, 80).addUnboundedFrom(80);
        searchSourceBuilder.aggregation(rangeAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Range range = resp.getAggregations().get("range_agg");
        System.out.println("-------------range----------------");
        for(Range.Bucket entry: range.getBuckets()){
            String key = entry.getKeyAsString();
            Number from = (Number)entry.getFrom();
            Number to = (Number)entry.getTo();
            System.out.println(key + " : from (" + from + ") to (" + to + ") --- " + entry.getDocCount());
        }
        System.out.println("-------------range----------------");

        DateRangeAggregationBuilder dateRangeAggregationBuilder = AggregationBuilders.dateRange("date_range_agg")
                .field("publish_time").format("yyyy-MM-dd")
                .addUnboundedTo("2013-01-01")
                .addRange("2013-01-01", "2017-01-01")
                .addUnboundedFrom("2017-01-01");
        searchSourceBuilder.aggregation(dateRangeAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Range dateRange = resp.getAggregations().get("date_range_agg");
        System.out.println("-------------date range----------------");
        for(Range.Bucket entry: dateRange.getBuckets()){
            String key = entry.getKeyAsString();
            String from = entry.getFromAsString();
            String to = entry.getToAsString();
            System.out.println(key + " : from (" + from + ") to (" + to + ") --- " + entry.getDocCount());
        }
        System.out.println("-------------date range----------------");

        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders.dateHistogram("date_histogram_agg")
                .field("publish_time").format("yyyy-MM-dd").calendarInterval(DateHistogramInterval.YEAR);
        searchSourceBuilder.aggregation(dateHistogramAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Histogram histogram = resp.getAggregations().get("date_histogram_agg");
        System.out.println("-------------date histogram----------------");
        for(Histogram.Bucket entry: histogram.getBuckets()){
            DateTime key = (DateTime) entry.getKey();
            String keyAsString = entry.getKeyAsString();
            System.out.println(key + " --- " + keyAsString + " --- " + entry.getDocCount());
        }
        System.out.println("-------------date histogram----------------");

        MissingAggregationBuilder missingAggregationBuilder = AggregationBuilders.missing("missing_agg").field("price");
        searchSourceBuilder.aggregation(missingAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        resp = restClient.search(searchRequest, REQUEST_OPTIONS_DEFAULT);
        Missing missing = resp.getAggregations().get("missing_agg");
        System.out.println(missing.getDocCount());

        GeoDistanceAggregationBuilder geoDistanceAggregationBuilder = AggregationBuilders
                .geoDistance("geo_agg", new GeoPoint(34.3412700000, 108.9398400000))
                .field("location").unit(DistanceUnit.KILOMETERS)
                .addUnboundedTo(500)
                .addRange(500, 1000)
                .addUnboundedFrom(1000);
        SearchRequest geoSearchRequest = new SearchRequest("geo");
        SearchSourceBuilder geoSearchSourceBuilder = new SearchSourceBuilder();
        geoSearchSourceBuilder.aggregation(geoDistanceAggregationBuilder);
        geoSearchRequest.source(geoSearchSourceBuilder);
        resp = restClient.search(geoSearchRequest, REQUEST_OPTIONS_DEFAULT);
        Range geoRange = resp.getAggregations().get("geo_agg");
        System.out.println("-------------geo distance----------------");
        for(Range.Bucket entry: geoRange.getBuckets()){
            String key = entry.getKeyAsString();
            Number from = (Number) entry.getFrom();
            Number to = (Number) entry.getTo();
            System.out.println(key + " --- " + from + " - " + to + " --- " +entry.getDocCount());
        }
        System.out.println("-------------geo distance----------------");

        @SuppressWarnings("unused")
        IpRangeAggregationBuilder ipRangeAggregationBuilder = AggregationBuilders.ipRange("ip_range_agg").field("ip")
                .addUnboundedTo("100.0.0.5").addUnboundedFrom("100.0.0.155");
//        Range ipRange = resp.getAggregations().get("ip_range_agg");
//        for(Range.Bucket entry: ipRange.getBuckets()){
//            String key = entry.getKeyAsString();
//            String from = entry.getFromAsString();
//            String to = entry.getToAsString();
//            System.out.println(key + " --- " + from + " - " + to + " --- " + entry.getDocCount());
//        }
    }



}