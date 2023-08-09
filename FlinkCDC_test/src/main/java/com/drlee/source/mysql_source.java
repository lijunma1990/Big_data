package com.drlee.source;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.shaded.zookeeper3.org.apache.jute.compiler.JString;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;


public class mysql_source {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource;
        mySqlSource = MySqlSource.<String>builder()
                .hostname("172.32.0.72")
                .port(3306)
                .databaseList(".*") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*"
                .tableList("*.*") // set captured table
                .username("root")
                .password("mysql")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MsqlSource")
                //set 4 parallel sourcce tasks
                .setParallelism(4)
                .print()
                .setParallelism(1);// use parallelism 1 for sink to keep message ordering

//        env.execute("Print MySQL Snapshot + Binlog");
    }
}
