package cn.fxbin.flink.utils;

import cn.fxbin.flink.entity.User;
import com.alibaba.fastjson.JSON;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Date;
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

    public final String TOPIC = "user";

    public void writeToKafka(int i) {

        KafkaProducer producer = new KafkaProducer<String, String>(getKafkaProperties());

        User user = User.builder()
                .id(i)
                .username("test" + i)
                .age(i)
                .createDate(new Date(1578734445368L))
                .build();

        ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(user));
        producer.send(record);

        System.out.println("发送数据: " + JSON.toJSONString(user));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 80; i < 100; i++) {

            Thread.sleep(300);
            writeToKafka(i);
        }
    }

    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "user-group");
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

}
