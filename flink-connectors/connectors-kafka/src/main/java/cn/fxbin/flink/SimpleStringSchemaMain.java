package cn.fxbin.flink;

import cn.fxbin.flink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Main
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 15:57
 */
public class SimpleStringSchemaMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = KafkaUtils.getKafkaProperties();
        String topic = KafkaUtils.TOPIC;

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProperties
        )).setParallelism(1);

//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProperties);
//        kafkaConsumer.setStartFromEarliest();     //从最早的数据开始消费
//        kafkaConsumer.setStartFromLatest();       //从最新的数据开始消费
//        kafkaConsumer.setStartFromTimestamp(...); //从根据指定的时间戳（ms）处开始消费
//        kafkaConsumer.setStartFromGroupOffsets(); //默认从提交的 offset 开始消费


        dataStreamSource.print();

        environment.execute("Flink add kafka data source");

    }

}
