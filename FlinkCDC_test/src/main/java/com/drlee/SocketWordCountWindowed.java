package com.drlee;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class SocketWordCountWindowed {
    public static void main(String[] args) throws Exception {
        final String hostname;
        final int port;
        try {
            final ParameterTool params;
            params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "172.32.0.72";
            port = params.has("port") ? params.getInt("port") : 9999;
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        //通过text对象转换得到新的DataStream对象，
        //转换逻辑是分隔每个字符串，取得的所有单词都创建一个WordWithCount对象
        DataStream<WordWithCount> windowCounts;
        windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                for (String word : s.split("\\s")) {  //按空格空字符切分单次
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
                .keyBy("word")//key为word字段
                .timeWindow(Time.seconds(5))    //五秒一次的翻滚时间窗口
                .reduce(new ReduceFunction<WordWithCount>() { //reduce策略
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });


        //单线程输出结果
        windowCounts.print().setParallelism(1);

        // 执行job
        env.execute("Flink Streaming Java API Skeleton");
    }

    //pojo
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
