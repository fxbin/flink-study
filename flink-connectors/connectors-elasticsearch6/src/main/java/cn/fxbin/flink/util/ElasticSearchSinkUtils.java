package cn.fxbin.flink.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ElasticsearchSinkUtils
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/19 10:31
 */
@UtilityClass
public class ElasticSearchSinkUtils {

    private List<HttpHost> httpHosts = new ArrayList<>();

    public final String ES_LIST = "192.168.73.36:9200";

    /**
     * addSink
     *
     * @author fxbin
     * @since 2020/1/19 10:47
     * @param clusterNodes elasticsearch hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism 并行数
     * @param data 数据
     * @param function func
     */
    public <T> void addSink(List<HttpHost> clusterNodes, int bulkFlushMaxActions, int parallelism,
                            SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> function) {

        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(clusterNodes, function);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * getEsAddresses
     *
     * @author fxbin
     * @since 2020/1/19 10:44
     * @param clusterNodes es 节点配置信息
     * @return java.util.List<org.apache.http.HttpHost>
     */
    public List<HttpHost> getEsAddresses(String clusterNodes) {

        Arrays.stream(clusterNodes.split(",")).forEach(node -> {
            try {
                String[] parts = StringUtils.split(node, ":");
                Assert.assertNotNull( "Must defined", parts);
                Assert.assertEquals("Must be defined as 'host:port'", 2, parts.length);
                if (parts[0].startsWith("http")) {
                    httpHosts.add(new HttpHost(parts[0], Integer.parseInt(parts[1]), "http"));
                } else {
                    httpHosts.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Invalid ES nodes " + "property '" + node + "'", e);
            }
        });
        return httpHosts;
    }

    public static void main(String[] args) {
        String a = "123";
        System.out.println(Arrays.toString(a.split(",")));
    }


}
