package dx.ict.jobs;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.sink.KafkaSinkMap;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.TopicMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;


/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/18 10:55
 * @Version 1.0
 */

/**
 * 待清洗的数据库表名字：
 * icity.omp_equipment_info
 * icity.omp_video_equipment_info
 * icity.t_parameter_info_business
 * icity.omp_supplier
 * icity.long_lat
 * icity.s_dept
 * icity.t_lessee_info
 * icity.imp_event_info
 * icity.imp_event_config
 * icity.imp_equipment_warn
 * icity.omp_r_equipment_algo_ability
 * <p>
 * 当前操作的数据库表名：
 * icity.omp_equipment_info
 */
public class FlinkETL_MysqlToKafka_icity {
    public static void main(String[] args) {

        /*
         * mysqlSource 配置
         * */
        String mysql_sourceName_header = "mysql_source";
        String mysql_database_name = "icity";
        String mysql_table_list = "omp_r_equipment_algo_ability," +
                "imp_equipment_warn," +
                "imp_event_config," +
                "imp_event_info," +
                "t_lessee_info," +
                "s_dept," +
                "omp_supplier," +
                "t_parameter_info_business," +
                "omp_video_equipment_info," +
                "omp_equipment_info";
        String mysql_hostname = "172.32.0.72";
        String mysql_port = "3306";
        String mysql_username = "root";
        String mysql_password = "mysql";
        String mysql_serverId = "";
        String mysql_DeserializationSchema = "MySqlDeserializationSchema";

        /*
         * kafkaSink 配置
         * */
        String kafka_SinkName_header = "kafka_sink";
        String kafka_brokers = "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092";
//        String kafka_topics="";//由mysql表自动生成
        String kafka_valueSerializationSchema = "MyKafkaSerializationSchema";
        String kafka_keySerializationSchema = "MyKafkaSerializationSchema";


        /*
         * 创建运行环境
         * */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置3的并行度
//        env.enableCheckpointing(5000);//设置5s的checkpoint间隔
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置自动的运行模式

        /*
         * 创建source和map配置实例
         * */
        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();//创建mysqlSource集
        final KafkaSinkMap kafkaSinkMap = new KafkaSinkMap();//创建kafkaSink集
        final TopicMap topicMap = new TopicMap(
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                "^ods_.*"
        );//创建topic集

        /*
         * 向source和map配置实例中插入配置数据
         * */
        String[] TABLE_NAME_LIST = mysql_table_list.split(",");
        for (String item : TABLE_NAME_LIST
        ) {
            String item_topic = "ods_".concat(mysql_database_name).concat("_").concat(item);
            mysqlSourceMap.add_source(
                    mysql_sourceName_header,
                    mysql_hostname,
                    mysql_port,
                    mysql_database_name,//默认为ljm_test
                    mysql_database_name.concat(".").concat(item),
                    mysql_username,
                    mysql_password,
                    mysql_serverId,
                    mysql_DeserializationSchema
                    //默认为JsonDebeziumDeserializationSchema，如果为自定义，则选项为"MySqlDeserializationSchema"
            );//插入mysql数据库连接配置
            kafkaSinkMap.add_sink(
                    kafka_SinkName_header,
                    kafka_brokers,
                    item_topic,
                    kafka_valueSerializationSchema,//default -> SimpleStringSchema
                    kafka_keySerializationSchema
            );//插入kafka连接配置sink（name:kafka_sink->topic_name）
        }


        /*
         * 生成source和sink的连接器组件实例
         * 生成对应数据流：
         * mysqlSource->kafkaSink
         * kafkaSource->hiveSink
         * */
        for (String item : TABLE_NAME_LIST
        ) {
            String item_topic = "ods_".concat(mysql_database_name).concat("_").concat(item);
            //生成mysqlSource实例
            final String source_name = mysql_sourceName_header.concat("->").concat(mysql_database_name).concat(".").concat(item);
            final MySqlSource<String> mySqlSource = mysqlSourceMap
                    .get_source(source_name)
                    .build();
            final DataStreamSource<String> mySqlSource_dataStream = env
                    .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), source_name);
            //生成kafkaSink实例
            final KafkaSink<String> kafkaSink = kafkaSinkMap.get_sink(kafka_SinkName_header.concat("->").concat(item_topic)).build();

            /*
             * transform
             * 转换对应的数据流至所需的形态
             * */
//        final SingleOutputStreamOperator<target_pojo> omp_r_equipment_algo_ability_dataStream;
//        omp_r_equipment_algo_ability_dataStream = <XXX>Source_dataStream.map(
//                new omp_r_equipment_algo_ability_map()
//        );

            /*
             * 对接数据流与sink端，开始执行动作：
             * mysqlSource->kafkaSink
             * kafkaSource->hiveSink
             * */
            System.out.println(">>>>>>>>>>>>>>>>>>>>" + mySqlSource_dataStream);
            mySqlSource_dataStream.sinkTo(kafkaSink);
        }

        /*
         * 执行
         * */
        try {
            env.execute("mysqlSource->kafkaSink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
