package cn.fxbin.flink;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBaseReadMain
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/20 11:09
 */
public class HBaseReadMain {

    /**
     * 表名
     */
    public static final String HBASE_TABLE_NAME = "fxbin";

    /**
     * 列族
     */
    static final byte[] INFO = "info".getBytes(ConfigConstants.DEFAULT_CHARSET);

    /**
     * 列名
     */
    static final byte[] BAR = "bar".getBytes(ConfigConstants.DEFAULT_CHARSET);

    public static void main(String[] args) {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.createInput(new TableInputFormat<Tuple2<String, String>>() {

            private Tuple2<String, String> reuse = new Tuple2<String, String>();

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(INFO, BAR);
                return scan;
            }

            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
                String val = Bytes.toString(result.getValue(INFO, BAR));
                reuse.setField(key, 0);
                reuse.setField(val, 1);
                return null;
            }
        }).filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1.startsWith(HBASE_TABLE_NAME);
            }
        }).print();

    }

}
