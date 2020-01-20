package cn.fxbin.flink.util;

import cn.fxbin.flink.event.ProductEvent;
import com.alibaba.fastjson.JSONObject;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * ProductUtils
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/19 18:04
 */
@UtilityClass
public class ProductUtils {

    public final String BROKER_LIST = "192.168.73.33:9092";

    public final String TOPIC = "product";

    public final Random random = new Random();

    public void main(String[] args) {

        KafkaProducer producer = new KafkaProducer<String, String>(getKafkaProperties());

        for (int i = 1; i <= 10000; i++) {
            ProductEvent product = ProductEvent.builder().id((long) i)
                    // 商品名称
                    .name("product" + i)
                    //商品价格（以分为单位）
                    .price(random.nextLong() / 10000000000000L)
                    //商品编码
                    .code("code" + i).build();

            ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSONObject.toJSONString(product));
            producer.send(record);
            System.out.println("发送数据: " + JSONObject.toJSONString(product));
        }
        producer.flush();
    }


    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "product-group");
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

}
