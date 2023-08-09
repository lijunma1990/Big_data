package flinkstu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author KevenHe
 * @create 2022/2/13 12:22
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String host = tool.get("hadoop102");
//        int port = tool.getInt("9999");
        DataStream<String> data = env.socketTextStream("hadoop102",9999);
        DataStream<Tuple2<String,Integer>> wordCountData = data
                .flatMap(new StreamWordCount.FlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountData.print().setParallelism(1);
        env.execute();
    }
    public static class FlatMapper implements FlatMapFunction<String, Tuple2<String,
            Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws
                Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
