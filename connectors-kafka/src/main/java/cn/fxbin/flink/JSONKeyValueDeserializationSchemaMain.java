package cn.fxbin.flink;

import cn.fxbin.flink.util.KafkaUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * Main
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 16:23
 */
public class JSONKeyValueDeserializationSchemaMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = KafkaUtils.getKafkaProperties();
        String topic = KafkaUtils.TOPIC;

        DataStreamSource<ObjectNode> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>(
                topic,
                // 可以控制是否需要元数据字段
                new JSONKeyValueDeserializationSchema(true),
                kafkaProperties
        ));

        dataStreamSource.print();

        environment.execute();
    }

}
