package cn.fxbin.flink.util;

import cn.fxbin.flink.entity.Metric;
import com.alibaba.fastjson.JSON;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * KafkaUtils
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 15:43
 */
@UtilityClass
public class KafkaUtils {

    public final String BROKER_LIST = "192.168.73.33:9092";

    public final String TOPIC = "metric";

    public void writeToKafka() {

        KafkaProducer producer = new KafkaProducer<String, String>(getKafkaProperties());

        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "fxbin");
        tags.put("host_ip", "127.0.0.1");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        Metric metric = Metric.builder()
                .timestamp(System.currentTimeMillis())
                .name("es-test")
                .tags(tags)
                .fields(fields)
                .build();

        ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(metric));
        producer.send(record);

        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }

    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "metric-group");
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

}
