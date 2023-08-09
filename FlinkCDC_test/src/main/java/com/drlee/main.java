package com.drlee;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class main {
    /*
     * author:lee
     * @param:
     * */
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStream<String> elementsSource = env.fromElements("java,scala,php,c++", "java,scala,php", "java,scala", "java");
        // 数据处理
        DataStream<String> flatMap = elementsSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String element, Collector<String> out) throws Exception {
                String[] wordArr = element.split(",");
                for (String word : wordArr) {
                    out.collect(word);
                }
            }
        });
        //DataStream 下边为DataStream子类
        SingleOutputStreamOperator<String> source = flatMap.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });
        // 4.数据输出
        source.print();
        // 5.执行程序
        try {
            env.execute("flink-hello-world");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
