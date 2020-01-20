package cn.fxbin.flink.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.Tuple2;

/**
 * RedisMapper
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/20 9:54
 */
public class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        // 指定 RedisCommand 的类型是 HSET，对应 Redis 中的数据结构是 HASH，设置 key = redis-sink
        return new RedisCommandDescription(RedisCommand.HSET, "redis-sink");
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data._1;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data._2;
    }
}
