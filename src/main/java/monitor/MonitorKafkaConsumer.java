package monitor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MonitorKafkaConsumer {

    static Random random = new Random();

    public static void main(String[] args) throws Exception {
        String index  = "monitor";
        String type  = "latency";
        String index2 = "dwd_pv_latency_monitor";
        String type2 ="pv_latency";
        ElastisearchHelper esHelper = new ElastisearchHelper();
        esHelper.initializBulkWrite();
//        consumer(index,type , esHelper);
        consumer(index2,type2 , esHelper);
        esHelper.close();
    }

    public static void consumer(String indexNmae,String typeName,ElastisearchHelper esHelper) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kfk013218.heracles.sohuno.com:9092,pdn096224.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,pdn096229.heracles.sohuno.com:9092,pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092" );
        props.put("group.id", "pv-test8");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("mediaai_logcollect_wap_pv_dwd"));
//        consumer.subscribe(Arrays.asList("mediaai_logcollect_wap_se_dwd"));
        long start_time  = Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()));
//        long start_time  = System.currentTimeMillis();
        List<JSONObject> list = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        long ii= 0l;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            long current_time  = Long.parseLong(sdf.format(new Date()));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.toString());
                JSONObject tmp = new JSONObject();
                tmp.put("log_time_stamp", JSONObject.parseObject(record.value()).getString("logTime"));
                tmp.put("current_time", current_time);

                list.add(tmp);
            }
            int threshold = 50;
            if(current_time - start_time >= 5000) {
                float average_time = 0f;

                for(int i = 0;i < threshold; i++) {
                    int index = random.nextInt( list.size()-1 );
                    JSONObject tmp = list.get(index);
                    long start = tmp.getLong("log_time_stamp");
                    long stop = tmp.getLong("current_time");
                    average_time += (stop - start);
                }

                JSONObject tmp = new JSONObject();
                tmp.put("averageLatency", (double)average_time/threshold );
                tmp.put("postDate",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date()));
                esHelper.persist(indexNmae, typeName, tmp );
                list.removeAll(list);
//                list = new ArrayList<>();
                start_time = Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()));
            }

        }
    }
}
