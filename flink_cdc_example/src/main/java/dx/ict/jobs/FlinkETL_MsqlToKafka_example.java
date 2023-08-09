package dx.ict.jobs;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.sink.KafkaSinkMap;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.TopicMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author 李二白
 * @date 2023/04/10
 */

public class FlinkETL_MsqlToKafka_example {
    public static void main(String[] args) {

        String databaseList = "ljm_test";
        String tableList = "ljm_test.user_info";
        String topicName = "ljm_test_user_info";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置 3s 的 checkpoint 间隔
//        env.enableCheckpointing(3000);

        final TopicMap topicMap = new TopicMap("172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092", "^ljm-test.*");
        final List<String> topic_list = topicMap.List_All_Topic();
        for (String item : topic_list
        ) {
            System.out.println("topic: " + item);
        }
        System.out.println(topicName.concat(">>>>>>>>>state default:" + topicMap.isExist(topicName)));
        topicMap.removeTopic(topicName);
        System.out.println(topicName.concat(">>>>>>>>>state on delete:" + topicMap.isExist(topicName)));
        topicMap.addTopic(topicName);
        topicMap.refresh();
        System.out.println(topicName.concat(">>>>>>>>>state on add:" + topicMap.isExist(topicName)));

        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();
        mysqlSourceMap.add_source(
                "mysql_source",
                "172.32.0.72",
                "3306",
                databaseList,//默认为ljm_test
                tableList,
                "root",
                "mysql",
                "",
                "MySqlDeserializationSchema"
                //默认为JsonDebeziumDeserializationSchema，如果为自定义，则选项为"MySqlDeserializationSchema"
        );

        //创建kafkasinkMap以及kafkasink
        final KafkaSinkMap kafkaSinkMap = new KafkaSinkMap();
        kafkaSinkMap.add_sink(
                "kafka_sink",
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                topicName,
                "MyKafkaSerializationSchema",//default -> SimpleStringSchema
                "MyKafkaSerializationSchema"
        );

        final KafkaSink<String> Kafka_dataStreamSink_collector72;
        Kafka_dataStreamSink_collector72 = kafkaSinkMap.get_sink("kafka_sink->".concat(topicName))
                .build();
//        kafkaSinkMap.remove_sinks("kafka_sink->fwb.*");
        final MySqlSource<String> MysqlSource_collector72;
        MysqlSource_collector72 = mysqlSourceMap.get_source("mysql_source->".concat(tableList))
                .build();
        final DataStreamSource<String> Mysql_dataStreamSource_collector72 = env.fromSource(
                MysqlSource_collector72,
                WatermarkStrategy.noWatermarks(),
                "collector@172.32.0.72");
        Mysql_dataStreamSource_collector72.print();
        System.out.println(">>>>>>>>>>>>>>>>>>>>" + Mysql_dataStreamSource_collector72);
        //输出dataStream类型的kafka的source
        Mysql_dataStreamSource_collector72.sinkTo(Kafka_dataStreamSink_collector72);

        try {
            env.execute("flink_cdc_mysql->kafka:".concat(tableList));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
