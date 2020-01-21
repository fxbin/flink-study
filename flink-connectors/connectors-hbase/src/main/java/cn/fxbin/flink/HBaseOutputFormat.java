package cn.fxbin.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBaseOutputFormat
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/21 11:46
 */
@Slf4j
public class HBaseOutputFormat implements OutputFormat<String> {

    private org.apache.hadoop.conf.Configuration configuration;
    private Connection connection = null;
    private String taskNumber = null;
    private Table table = null;
    private int rowNumber = 0;

    @Override
    public void configure(Configuration parameters) {
        //设置配置信息
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.73.37:2181");
//        configuration.set("hbase.zookeeper.property.clientPort", "");
        configuration.set("hbase.rpc.timeout", "30000");
        configuration.set("hbase.client.operation.timeout", "30000");
        configuration.set("hbase.client.scanner.timeout.period", "30000");

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("fxbin");
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            log.info("==============不存在表 = {}", tableName);
            admin.createTable(new HTableDescriptor(TableName.valueOf("fxbin"))
                    .addFamily(new HColumnDescriptor("hello, name")));

        }
        table = connection.getTable(tableName);
        this.taskNumber = String.valueOf(taskNumber);
    }

    @Override
    public void writeRecord(String record) throws IOException {
        Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
        put.addColumn(Bytes.toBytes("hello"), Bytes.toBytes("fxbin"),
                Bytes.toBytes(String.valueOf(rowNumber)));
        rowNumber++;
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
