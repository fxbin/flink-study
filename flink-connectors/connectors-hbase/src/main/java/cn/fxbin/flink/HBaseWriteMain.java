package cn.fxbin.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.util.Arrays;

/**
 * HBaseWriteMain 写入数据到 HBase
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/20 11:21
 */
public class HBaseWriteMain {

    //表名
    public static final String HBASE_TABLE_NAME = "fxbin_sink";
    // 列族
    static final byte[] INFO = "info_sink".getBytes(ConfigConstants.DEFAULT_CHARSET);
    //列名
    static final byte[] BAR = "bar_sink".getBytes(ConfigConstants.DEFAULT_CHARSET);

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "The fair is be in that orisons"
    };

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        Job job = Job.getInstance();
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);

        // java.lang.IllegalArgumentException: Can not create a Path from a null string
        // 解决方式
        job.getConfiguration().set("mapred.output.dir", "/tmp");

        environment.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");

                        Arrays.stream(splits).filter(str -> str.length() > 0).forEach(str -> {
                            out.collect(new Tuple2<>(str, 1));
                        });
                    }
                })
                .groupBy(0)
                .sum(1)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<Text, Mutation>>() {

                    private transient Tuple2<Text, Mutation> reuse;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        reuse = new Tuple2<Text, Mutation>();
                    }

                    @Override
                    public Tuple2<Text, Mutation> map(Tuple2<String, Integer> value) throws Exception {
                        reuse.f0 = new Text(value.f0);
                        Put put = new Put(value.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));
                        put.addColumn(INFO, BAR, Bytes.toBytes(value.f1.toString()));
                        reuse.f1 = put;
                        return reuse;
                    }
                }).output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));

        environment.execute("flink connectors hbase sink");

    }

}
