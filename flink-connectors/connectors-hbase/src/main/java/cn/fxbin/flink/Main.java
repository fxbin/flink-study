package cn.fxbin.flink;

import cn.fxbin.flink.util.ExecutionEnvUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import static cn.fxbin.flink.contant.HBaseConstants.*;

/**
 * Main
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/21 11:56
 */
public class Main {

    /**
     * 表名
     */
    private static TableName HBASE_TABLE_NAME = TableName.valueOf("fxbin_stream");

    /**
     * 列族
     */
    private static final String INFO_STREAM = "info_stream";

    /**
     * 列名
     */
    private static final String BAR_STREAM = "bar_stream";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtils.createParameterTool(args);
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = environment.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 1L;
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<String> out) throws Exception {
                while (isRunning) {
                    out.collect(String.valueOf(Math.floor(Math.random() * 100)));
                }
            }
            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        dataStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String string) throws Exception {
                writeEventToHbase(string, parameterTool);
                return string;
            }
        }).print();

        environment.execute("flink connectors hbase");
    }

    private static void writeEventToHbase(String string, ParameterTool parameterTool) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_ZOOKEEPER_QUORUM, parameterTool.get(HBASE_ZOOKEEPER_QUORUM));
        configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, parameterTool.get(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
        configuration.set(HBASE_RPC_TIMEOUT, parameterTool.get(HBASE_RPC_TIMEOUT));
        configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, parameterTool.get(HBASE_CLIENT_OPERATION_TIMEOUT));
        configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, parameterTool.get(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));

        Connection connect = ConnectionFactory.createConnection(configuration);
        Admin admin = connect.getAdmin();
        if (!admin.tableExists(HBASE_TABLE_NAME)) { //检查是否有该表，如果没有，创建
            admin.createTable(new HTableDescriptor(HBASE_TABLE_NAME).addFamily(new HColumnDescriptor(INFO_STREAM)));
        }
        Table table = connect.getTable(HBASE_TABLE_NAME);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        Put put = new Put(Bytes.toBytes(date.getTime()));
        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes("test"), Bytes.toBytes(string));
        table.put(put);
        table.close();
        connect.close();
    }

}
