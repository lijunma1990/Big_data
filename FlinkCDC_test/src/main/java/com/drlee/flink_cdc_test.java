package org.drlee.test;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**FlinkCDC
 *
 * @author Drlee
 * @date 2023/03/17
 * */
public class flink_cdc_test {
    public static void main(String[] args) {
        //1.获得flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("172.32.0.72")
                .port(3306)
                .username("root")
                .password("mysql")
                .databaseList("fwb_test")	//监控的数据库
                .tableList("fwb_test.user_info")	//监控的数据库下的表
                .deserializer(new StringDebeziumDeserializationSchema())//反序列化
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();

        //4.启动任务
        try {
            env.execute("FlinkCDC_test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
