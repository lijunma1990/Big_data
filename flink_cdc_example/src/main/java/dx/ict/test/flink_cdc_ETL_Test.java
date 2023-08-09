package dx.ict.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.sink.KafkaSinkMap;
import dx.ict.source.KafkaSourceMap;
import dx.ict.source.MysqlSourceMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class flink_cdc_ETL_Test {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置 3s 的 checkpoint 间隔
//        env.enableCheckpointing(3000);

        /**
         * @example :
        public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("172.32.0.72")
        .port(3306)
        .databaseList("ljm_test") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
        .tableList("ljm_test.user_info") // 设置捕获的表
        .username("root")
        .password("mysql")
        .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 3s 的 checkpoint 间隔
        //        env.enableCheckpointing(3000);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        // 设置 source 节点的并行度为 4
        .setParallelism(4)
        .print().setParallelism(1); // 设置 sink 节点并行度为 1
        env.execute("Print MySQL Snapshot + Binlog");
        }
        }
         */

        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();


        mysqlSourceMap.add_source(
                "collector@172.32.0.72",
                "172.32.0.72",
                "3306",
                "icity,ljm_test",//默认为ljm_test
                "ljm_test.*",
                "root",
                "mysql",
                "",
                "MySqlDeserializationSchema"
                //默认为JsonDebeziumDeserializationSchema，如果为自定义，则选项为"MySqlDeserializationSchema"
        );
        final MySqlSource<String> MysqlSource_collector72;
        MysqlSource_collector72 = mysqlSourceMap.get_source("collector@172.32.0.72")
                .build();
        final DataStreamSource<String> Mysql_dataStreamSource_collector72 = env.fromSource(MysqlSource_collector72, WatermarkStrategy.noWatermarks(), "collector@172.32.0.72");
        Mysql_dataStreamSource_collector72.print();
        System.out.println(">>>>>>>>>>>>>>>>>>>>" + Mysql_dataStreamSource_collector72);
        //输出dataStream类型的kafka的source

        //创建kafkasinkMap以及kafkasink
        final KafkaSinkMap kafkaSinkMap = new KafkaSinkMap();
        kafkaSinkMap.add_sink(
                "kafka_sink",
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                "ljm-test-user-info",
                "MyKafkaSerializationSchema",//default -> SimpleStringSchema
                "MyKafkaSerializationSchema"
        );
        final KafkaSink<String> Kafka_dataStreamSink_collector72;
        Kafka_dataStreamSink_collector72 = kafkaSinkMap.get_sink("kafka_sink")
                .build();

        Mysql_dataStreamSource_collector72.sinkTo(Kafka_dataStreamSink_collector72);


//        //创建KafkaSourceMap以及Kafkasource
//        final KafkaSourceMap kafkaSourceMap = new KafkaSourceMap();
//        kafkaSourceMap.add_source(
//                "kafka_source",
//                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
//                "ods_icity_imp_repair_work",
//                "",
//                "Flink-ETL-Test",
//                "earliest",
//                "MyKafkaDeserializationSchema_Json"//default -> SimpleStringSchema
//        );
//        final KafkaSource<String> Kafka_dataStreamSource;
//        Kafka_dataStreamSource = kafkaSourceMap.get_source("kafka_source")
//                .build();
//        final DataStreamSource<String> Kafka_dataStreamSource_collector72;
//        Kafka_dataStreamSource_collector72 = env.fromSource(Kafka_dataStreamSource, WatermarkStrategy.noWatermarks(), "Kafka_dataStreamSource_collector72");
//
//        System.out.println(">>>>>>>>>>>>>>>>>>>>" + Kafka_dataStreamSource_collector72);
//        //输出dataStream类型的kafka的source
//
//        Kafka_dataStreamSource_collector72.print();


        try {
            env.execute("flink_cdc_test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
