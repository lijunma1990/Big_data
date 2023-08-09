package com.keven.mall.realtime.test;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.google.inject.internal.asm.$ByteVector;
import com.keven.mall.realtime.app.function.CustomerDeserialization;
import com.keven.mall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author KevenHe
 * @create 2022/2/7 14:52
 */
public class FlinkCDC  {
    public static void main(String[] args) throws Exception {
        //1.构建环境
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.setParallelism(1);
        //2.FlinkCDC创建sourceFunction并读取数据
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest()).build();
        DataStreamSource source = env.addSource(mysqlSource);
        //3. 打印数据并写入到kafka
        String sink_topic ="ods_base_log";
        source.addSink(MyKafkaUtil.getKafkaProducer(sink_topic));
        //4. 启动任务
        env.execute();
    }
}
