package ElasticsearchJavaApiTest;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestCluster {

    private static RestHighLevelClient restClient = null;
    private static RequestOptions REQUEST_OPTIONS_DEFAULT = null;

    @BeforeAll
    static void beforeAll() throws IOException {
        restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        REQUEST_OPTIONS_DEFAULT = RequestOptions.DEFAULT;
    }
    /**
     * 集群管理
     */
    @Test
    @Order(1)
    public void clusterHealth() throws IOException {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
        clusterHealthRequest.indices("books", "geo");

        ClusterHealthResponse healthResp = restClient.cluster().health(clusterHealthRequest, REQUEST_OPTIONS_DEFAULT);

        String clusterName = healthResp.getClusterName();
        System.out.println("cluster name: " + clusterName);

        ClusterHealthStatus clusterHealthStatus = healthResp.getStatus();
        System.out.println("cluster status: " + clusterHealthStatus.toString());

        boolean timedOut = healthResp.isTimedOut();
        RestStatus restStatus = healthResp.status();
        System.out.println("cluster timeout: " + timedOut + ", restStatus: " + restStatus.toString());

        System.out.println("cluster nodes number: " + healthResp.getNumberOfNodes() + ", data nodes number: " + healthResp.getNumberOfDataNodes());
        System.out.println("cluster active shards: " + healthResp.getActiveShards());
    }
    @Test
    @Order(2)
    public void clusterGetSetting() throws IOException{
        ClusterGetSettingsRequest clusterGetSettingsRequest = new ClusterGetSettingsRequest();
        clusterGetSettingsRequest.includeDefaults(true); // true 返回默认设置
        ClusterGetSettingsResponse settingsResp = restClient.cluster().getSettings(clusterGetSettingsRequest, REQUEST_OPTIONS_DEFAULT);

        Settings persistentSettings = settingsResp.getPersistentSettings();
        Settings transientSettings = settingsResp.getTransientSettings();
        Settings defaultSettings = settingsResp.getDefaultSettings();
        String settingValue = settingsResp.getSetting("cluster.routing.allocation.enable");
        System.out.println(String.format("persistentSettings：%s", persistentSettings.toString()));
        System.out.println(String.format("transientSettings：%s", transientSettings.toString()));
        System.out.println(String.format("defaultSettings：%s", defaultSettings.toString()));
        System.out.println(String.format("settingValue：%s", settingValue));
        System.out.println(settingValue);
    }
}
