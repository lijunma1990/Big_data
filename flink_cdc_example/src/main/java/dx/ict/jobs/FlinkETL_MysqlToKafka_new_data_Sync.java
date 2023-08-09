package dx.ict.jobs;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.sink.KafkaSinkMap;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.TopicMap;
import dx.ict.util.tableRowTypeMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/18 10:54
 * @Version 1.0
 */
public class FlinkETL_MysqlToKafka_new_data_Sync {
    /**
     * flink cdc Mysql整库同步
     */


    private static final Logger log = LoggerFactory.getLogger(FlinkETL_MysqlToKafka_new_data_Sync.class);

    public static void main(String[] args) throws Exception {

        // source端连接信息
        String mysql_SourceName_header = "mysql_source";
        String userName = "root";
        String passWord = "mysql";
        String host = "172.32.0.72";
        String db = "new_data";
        // 如果是整库，tableList = ".*"
        String tableList = ".*";
        int port = 3306;
        String MySqlDeserializationSchema = "MySqlDeserializationSchema_Tuple5";


        // sink连接信息模板
        String sink_db = "";
        String sink_username = "root";
        String sink_password = "mysql";
        String sink_host = "172.32.0.72";
        String sink_port = "3306";
        String sink_url_template = "jdbc:mysql://${sink_host}:${sink_port}/${sink_db}?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";

        /*
         * kafkaSink 配置
         * */
        String kafka_SinkName_header = "kafka_sink";
        String kafka_brokers = "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092";
        String kafka_topics = "MysqlToKafka_Sync_".concat(db);
        String kafka_valueSerializationSchema = "MyKafkaSerializationSchema";
        String kafka_keySerializationSchema = "MyKafkaSerializationSchema";

        //编写sink映射表with语句
        final String sink_url = sink_url_template
                .replace("${sink_db}", sink_db)
                .replace("${sink_host}", sink_host)
                .replace("${sink_port}", sink_port);
        String connectorWithBody =
                " with (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = '${sink_url}',\n" +
                        " 'username' = '${sink_username}',\n" +
                        " 'password' = '${sink_password}',\n" +
                        " 'table-name' = '${tableName}'\n" +
                        ")";
        connectorWithBody = connectorWithBody.replace("${sink_url}", sink_url)
                .replace("${sink_username}", sink_username)
                .replace("${sink_password}", sink_password);

        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * 默认分区器会有2个坑：
         * 当 Sink 的并发度低于 Topic 的 partition 个数时，一个 sink task 写一个 partition，会导致部分 partition 完全没有数据。
         * 当 topic 的 partition 扩容时，则需要重启作业，以便发现新的 partition。
         * */
        env.setParallelism(3);
        //开启检查点3S
        env.enableCheckpointing(3000);
        //生成表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册同步的库对应的catalog
        MySqlCatalog mysqlCatalog = new MySqlCatalog("mysql_catalog", db, userName, passWord, String.format("jdbc:mysql://%s:%d", host, port));
        //判断该数据库是否存在
        log.info("If database exists:{}", mysqlCatalog.databaseExists(db));
        /*
         * 从mysqlCatalog获取RowTypeInfo
         * 创建表名和对应RowTypeInfo映射的Map
         * tableTypeInformationMap:Map<String, RowTypeInfo> ->存放表字段类型信息
         * tableDataTypesMap:Map<String, DataType[]> ->存放表字段类型
         * tableRowTypeMap:Map<String, RowType> ->存放表字段类型**
         * */
        final tableRowTypeMap tableRowTypeMap_O = new tableRowTypeMap(mysqlCatalog, tableList, db);
        final Map<String, RowType> tableRowTypeMap = tableRowTypeMap_O.getTableRowTypeMap();
        final Map<String, RowTypeInfo> tableTypeInformationMap = tableRowTypeMap_O.getTableTypeInformationMap();
        final List<String> primaryKeys = tableRowTypeMap_O.getPrimaryKeys();
        final List<String> tables = tableRowTypeMap_O.getTables();
        final Map<String, String[]> fieldNamesMap = tableRowTypeMap_O.getFieldNamesMap();
        final Map<String, DataType[]> tableDataTypesMap = tableRowTypeMap_O.getTableDataTypesMap();
        //打印源库中的表
        System.out.println("Source database tables : >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(tables);


        /*
         * 创建source和map配置实例
         * */
        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();//创建mysqlSource集
        final KafkaSinkMap kafkaSinkMap = new KafkaSinkMap();//创建kafkaSink集
        final TopicMap topicMap = new TopicMap(
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                "^.*"
        );//创建topic集


        //查看topic：kafka_topics是否存在，若不存在，则创建之
        String[] kafka_topic_list = kafka_topics.split(",");
        for (String item : kafka_topic_list
        ) {
            if (!topicMap.isExist(item)) {
                topicMap.addTopic(item, 3);
                log.info("create kafka Topic:{}", item);
            }
        }

        // 监控mysql binlog/生成mysql的source
        mysqlSourceMap.add_source(
                mysql_SourceName_header,
                host,
                String.valueOf(port),
                db,
                tableList,
                userName,
                passWord,
                "",
                MySqlDeserializationSchema);//插入Mysql连接配置source（name:mysql_source->tableList）
        String mysqlSourceName = mysql_SourceName_header.concat("->").concat(tableList);
        MySqlSource MysqlSource = mysqlSourceMap.get_source(mysqlSourceName, tableRowTypeMap)
                .build();
        kafkaSinkMap.add_sink(
                kafka_SinkName_header,
                kafka_brokers,

                kafka_topics,
                kafka_valueSerializationSchema,//default -> SimpleStringSchema
                kafka_keySerializationSchema
        );//插入kafka连接配置sink（name:kafka_sink->kafka_topics）
        String kafkaSinkName = kafka_SinkName_header.concat("->").concat(kafka_topics);
        KafkaSink kafkaSink = kafkaSinkMap.get_sink(kafkaSinkName).build();

        /*
         * MysqlSourceStream : MysqlSourceStream<Tuple2<String, Row>>
         * 对接数据流与sink端，开始执行动作：
         * MysqlSource->kafkaSink
         * kafkaSource->hiveSink
         * */
        final DataStreamSource MysqlSourceStream = env.fromSource(MysqlSource, WatermarkStrategy.noWatermarks(), mysqlSourceName);
        MysqlSourceStream.sinkTo(kafkaSink);

        env.execute("FlinkETL_MysqlToKafka_new_data_Sync");
    }
}
