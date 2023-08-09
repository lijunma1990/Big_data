package com.drlee;

import com.drlee.pojo.WaterSensor;
import com.drlee.sinks.RetractStream_Mysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * created by Drlee on 2023-03-16
 * 从socket端接收数据，并设置30秒触发执行一次窗口运算
 */

public class Flink_Group_Window_Tumble_Sink_Mysql_job {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999, "\n");
        SingleOutputStreamOperator<WaterSensor> waterDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

// 将流转化为表
        Table table = tableEnv.fromDataStream(waterDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        tableEnv.createTemporaryView("EventTable", table);

        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "id, " + //window_start, window_end,
                        "COUNT(ts) ,SUM(ts)" +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE EventTable , " +
                        "DESCRIPTOR(pt), " +
                        "INTERVAL '30' SECOND)) " +
                        "GROUP BY id , window_start, window_end"
        );

        tableEnv.toRetractStream(result, Row.class).addSink(new RetractStream_Mysql());
        env.execute();
    }
}
