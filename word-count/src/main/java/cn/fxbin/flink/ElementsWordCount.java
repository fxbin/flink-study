package cn.fxbin.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * ArrayWordCount
 *
 * @author fxbin
 * @version v1.0
 * @since 2019/12/5 17:14
 */
public class ElementsWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        SingleOutputStreamOperator<Tuple2<String, Long>> result = env.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");
                        Arrays.stream(splits).forEach(word -> collector.collect(new Tuple2<>(word, 1L)));
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) throws Exception {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });

        result.print();

        env.execute();
    }

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer"
    };

}
