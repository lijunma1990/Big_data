package dx.ict.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import dx.ict.source.MysqlSourceMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkETL_Mysql {
    public static void main(String[] args) {

        String catalogName = "myhive";
        String defaultDatabase = "temp";
        String hiveConfDir = "/etc/hive/conf.cloudera.hive";
        String hiveVersion = "2.3.9";

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境，并行度应设置为kafka主题的分区数

/*
        //生产环境下使用：
        //1.1 开启checkpoint
        env.enableCheckpointing(5*6000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10*6000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://172.32.0.67:9870/Flink-CDC/CheckPoint");
        System.setProperty("HADOOP_USER_NAME","atguigu");//配置hadoop的HDFS账号和密码

*/


        //构建mysql的SourceFunction，并读取数据
        MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();
        mysqlSourceMap.add_source(
                "collector@172.32.0.72",
                "172.32.0.72",
                "3306",
                "icity,ljm_test",
                "ljm_test.*",
                "root",
                "mysql",
                "",
                "MySqlDeserializationSchema"
        );

        final MySqlSource<String> mySqlSource_72;
        mySqlSource_72 = mysqlSourceMap.get_source("collector@172.32.0.72")
                .build();

        // 将源数据读取成为流数据
        DataStreamSource<String> mysqlStreamSource = env
                .fromSource(mySqlSource_72, WatermarkStrategy.noWatermarks(), "mysql_collector_72");
        mysqlStreamSource.print();


//        mysqlStreamSource.print();


        try {
            env.execute("xxx_test");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
