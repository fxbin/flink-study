package cn.fxbin.flink;

import cn.fxbin.flink.entity.User;
import cn.fxbin.flink.source.SinkToMysql;
import cn.fxbin.flink.utils.KafkaUtils;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * SaveDataFromKafkaToMysqlMain
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 17:17
 */
public class SaveDataFromKafkaToMysqlMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = KafkaUtils.getKafkaProperties();
        String topic = KafkaUtils.TOPIC;

        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = environment.addSource(new FlinkKafkaConsumer<>(
                topic,
                // 可以控制是否需要元数据字段
                new SimpleStringSchema(),
                kafkaProperties
        )).setParallelism(1)
                .map(msg -> JSON.parseObject(msg, User.class));


        // J数据入mysql
        userSingleOutputStreamOperator.addSink(new SinkToMysql());

        environment.execute();
    }

}
