package cn.fxbin.flink;

import cn.fxbin.flink.source.SourceFromMysql;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 17:02
 */
public class ReadDataFromMysqlMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMysql()).print();

        env.execute("Flink add data sourc");
    }
}
