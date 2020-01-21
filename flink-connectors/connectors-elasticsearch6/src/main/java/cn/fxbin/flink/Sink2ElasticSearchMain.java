package cn.fxbin.flink;

import cn.fxbin.flink.entity.Metric;
import cn.fxbin.flink.util.ElasticSearchSinkUtils;
import cn.fxbin.flink.util.KafkaUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Properties;

import static com.alibaba.fastjson.JSON.*;

/**
 * Sink2ElasticsearchMain
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/19 10:53
 */
public class Sink2ElasticSearchMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 读取数据
        Properties kafkaProperties = KafkaUtils.getKafkaProperties();
        String topic = KafkaUtils.TOPIC;

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProperties
        )).setParallelism(1);


        List<HttpHost> httpHostList = ElasticSearchSinkUtils.getEsAddresses(ElasticSearchSinkUtils.ES_LIST);
        ElasticSearchSinkUtils.addSink(httpHostList, 3000, 2,
                dataStreamSource,
                (String metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(JSON.parseObject(metric, Metric.class).getName())
                            .type(JSON.parseObject(metric, Metric.class).getName())
                            .source(toJSONString(metric), XContentType.JSON));
                });

        environment.execute("flink connectors elasticsearch");

    }

}
