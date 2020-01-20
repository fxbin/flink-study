package cn.fxbin.flink;

import cn.fxbin.flink.event.ProductEvent;
import cn.fxbin.flink.mapper.RedisSinkMapper;
import cn.fxbin.flink.util.ProductUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;
import scala.Tuple2;

import java.util.Properties;

/**
 * Main
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/20 9:58
 */
public class RedisSinkMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka config
        Properties kafkaProperties = ProductUtils.getKafkaProperties();
        String topic = ProductUtils.TOPIC;

        // redis config
        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder().setHost("20.20.0.36").build();

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProperties
        )).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, String>> map = dataStreamSource
                .map(str -> JSON.parseObject(str, ProductEvent.class))
                .flatMap(new FlatMapFunction<ProductEvent, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(ProductEvent value, Collector<Tuple2<String, String>> out) throws Exception {
                        // 收集商品 id 和 price 两个属性
                        out.collect(new Tuple2<>(value.getId().toString(), value.getPrice().toString()));
                    }
                });

        map.addSink(new RedisSink<>(poolConfig, new RedisSinkMapper()));

        map.print();

        environment.execute("flink redis connectors");
    }

}
