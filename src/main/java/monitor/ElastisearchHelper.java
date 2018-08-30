package monitor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author: xiliang
 * @Date: 2018/8/29 22:01
 **/

public class ElastisearchHelper {
    private static final String Host = "x.x.x.x";
    private static final int Http_Port = 9200;
    private BulkProcessor bulkProcessor;
    private static RestHighLevelClient client;

    static {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(Host, Http_Port, "http"), new HttpHost(Host, 9201, "http")));
    }

    public static void Search(String index, String type) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        searchSourceBuilder.from(9977);
//        searchSourceBuilder.size(20);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.toString());
        if (client != null) client.close();
    }

    public void initializBulkWrite() {
        System.out.println("begin initESPersistor.....");
        BulkWrite();
        System.out.println("finish initESPersistor.....");
    }


    public void BulkWrite() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            private Map<Long, Long> startMap = Maps.newHashMap();
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                startMap.put(executionId, System.currentTimeMillis());
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                Long start = startMap.remove(executionId);
                System.out.println(String.format("Finish Bulk, executionId = %s, Cost = %sms, Count = %s, Size = %s",
                        executionId, start == null ? -1 : (System.currentTimeMillis() - start), request.numberOfActions(), request.estimatedSizeInBytes()));
            }
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println(String.format("Error executing Bulk, executionId = %s, Count = %s, Size = %s",
                        executionId, request.numberOfActions(), request.estimatedSizeInBytes()));
            }
        };
        bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener)
                .setBulkActions(500)
                .setBulkSize(new ByteSizeValue(2L, ByteSizeUnit.MB))
                .setConcurrentRequests(0)
                .setFlushInterval(TimeValue.timeValueSeconds(5L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                .build();
    }


    public void persist(String indexName, String typeName, JSONObject jsonObject) {
        bulkProcessor.add(new IndexRequest(indexName, typeName).source(jsonObject));
    }


    public static void WriteIndex(String index, String type, JSONObject object) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, type).source(object);
        IndexResponse response1 = null;
        try {
            response1 = client.index(indexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response1.toString());
        if (client != null) client.close();
    }


    public static void CreateIndex(String indexName, String typeName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder().put("index.number_of_shards", 8)
                                            .put("index.number_of_replicas", 1));
        request.mapping(typeName,
                "{\n" +
                            "\"" +typeName + "\": {\n" +
                                    "\"properties\": {\n" +
                                    "\"log_time_stamp\": {\"type\": \"text\"}\n"+
//                                    "\"log_time_stamp\": {\"type\": \"text\"},\n"+
//                                    "\"postDate\": {\"type\": \"date\"},\n"+
//                                    "\"timeLatency\": {\"type\": \"integer\"}\n"+
                                "}\n"+
                            " }\n"+
                        "}\n"
                ,XContentType.JSON);
//        request.alias( new Alias(indexName+"_alias") );
        request.timeout(TimeValue.timeValueMinutes(2));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.waitForActiveShards(2);
        CreateIndexResponse createIndexResponse = null;
        try {
            createIndexResponse = client.indices().create(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("createIndexResponse:" + acknowledged + " , shardsAcknowledged:" + shardsAcknowledged);
        if (client != null) client.close();
    }

    public void Info() throws IOException {
        MainResponse response = null;
        try {
            response = client.info();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ClusterName clusterName = response.getClusterName();
        String clusterUuid = response.getClusterUuid();
        String nodeName = response.getNodeName();
        Version version = response.getVersion();
        Build build = response.getBuild();
        System.out.printf("clusterName:%s, clusterUuid:%s, nodeName:%s, version:%s, build:%s.", clusterName.toString(), clusterUuid, nodeName, version.toString(), build.toString());
        if (client != null) client.close();
    }

    public void DeleteIndex(String indexName){
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        try {
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(request);
            boolean acknowledged = deleteIndexResponse.isAcknowledged();
            System.out.println("DeleteIndex -> acknowledged:"+acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    public void close() {
		System.out.println("Closing bulkProcessor...");
		try {
			bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Closing ESClient...");
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeClient() {
        System.out.println("Closing ESClient...");
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args ){
        ElastisearchHelper es = new ElastisearchHelper();


//        String indexName = "monitor";
//        String typeName  = "latency";
        String index2 = "dwd_pv_latency_monitor";
        String type2 ="pv_latency";

        //        //####### createIndex
        try {
            CreateIndex(index2,type2);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        //### search
//        try {
//            new ElastisearchHelper().Search(index2,type2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        //##delete index
//        es.DeleteIndex(index2);

        es.closeClient();
    }

}
