package org.drlee.mysqlTest;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**FlinkCDC
 *
 * @author Drlee
 * @date 2023/03/17
 * */
public class flinkCdcTest {
    public static void main(String[] args) {
        //1.获得flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //1.1开启checkpoint
        env.enableCheckpointing(5000);//5秒钟
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setStateBackend(new FsStateBackend("hdfs://172.32.0.67:8020/cdc-test/ck"));


        //通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("172.32.0.72")
                .port(3306)
                .username("root")
                .password("mysql")
                .databaseList("ljm_test")	//监控的数据库
                .tableList("ljm_test.user_info")	//监控的数据库下的表
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
