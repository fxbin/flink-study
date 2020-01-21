package cn.fxbin.flink.contant;

/**
 * HBaseConstants
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/21 11:55
 */
public interface HBaseConstants {
    
    String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    String HBASE_CLIENT_RETRIES_NUMBER = "hbase.client.retries.number";
    String HBASE_MASTER_INFO_PORT = "hbase.master.info.port";
    String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";
    String HBASE_CLIENT_OPERATION_TIMEOUT = "hbase.client.operation.timeout";
    String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = "hbase.client.scanner.timeout.period";

    String HBASE_TABLE_NAME = "hbase.table.name";
    String HBASE_COLUMN_NAME = "hbase.column.name";
    
}
