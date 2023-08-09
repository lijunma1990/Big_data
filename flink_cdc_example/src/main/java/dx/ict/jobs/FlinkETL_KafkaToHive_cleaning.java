package dx.ict.jobs;

import dx.ict.enumeration.FlinkETL_FilterEnum;
import dx.ict.enumeration.FlinkETL_MapperEnum;
import dx.ict.pojo.omp_r_equipment_algo_ability;
import dx.ict.source.KafkaSourceMap;
import dx.ict.util.HiveCatalogMap;
import dx.ict.util.TopicMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;


/**
 * @author 李二白
 * @date 2023/04/04
 * <p>
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
public class FlinkETL_KafkaToHive_cleaning {
    public static void main(String[] args) {

        /*
         * hiveSink 配置
         * */
        String CATALOG_NAME = "hiveCatalog";
        String hiveConfDir =
                "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.hive\\";
        //集群配置："/etc/hive/conf.cloudera.hive";
        String hadoopConfDir = "";
        String hiveVersion = "2.3.9";
        String sink_database = "ods_icity";

        /*
         * kafkaSource 配置
         * */
        String SourceName_header = "kafka_source";
        String brokers = "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092";
        String topics = "ods_icity_imp_equipment_warn," +
                "ods_icity_imp_event_config," +
                "ods_icity_imp_event_info," +
                "ods_icity_omp_equipment_info," +
                "ods_icity_omp_r_equipment_algo_ability," +
                "ods_icity_omp_supplier," +
                "ods_icity_omp_video_equipment_info," +
                "ods_icity_s_dept," +
                "ods_icity_t_lessee_info," +
                "ods_icity_t_parameter_info_business";
        String topicPattern = "";//example:"^ods_icity_.*"
        String groupId = "Flink_ETL_Clean";
        String offsets = "earliest";
        String deserializerSchema = "MyKafkaDeserializationSchema_String";//default -> SimpleStringSchema;

        /*
         * 创建运行环境
         * */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置X的并行度
//        env.enableCheckpointing(5000);//设置5s的checkpoint间隔
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//设置自动的运行模式

        /*
         * 创建source和map配置实例
         * */
        String[] topic_list = topics.split(",");
        final KafkaSourceMap kafkaSourceMap = new KafkaSourceMap();//创建kafkaSource集
        final HiveCatalogMap hiveCatalogMap = new HiveCatalogMap();//创建hiveCatalog集
        final TopicMap topicMap = new TopicMap(
                brokers,
                "^".concat(sink_database).concat("_.*")
        );//创建topic集

        /*
         * 向source和map配置实例中插入配置数据
         * */
        for (String item : topic_list
        ) {
            kafkaSourceMap.add_source(
                    SourceName_header,
                    brokers,
                    item,
                    topicPattern,
                    groupId,
                    offsets,
                    deserializerSchema//default -> SimpleStringSchema
            );//插入kafka连接配置source
        }
        hiveCatalogMap.add_Catalog(
                CATALOG_NAME,
                sink_database,
                hiveConfDir,
                hadoopConfDir,
                hiveVersion
        );//插入hiveCatalog配置


        /*
         * 生成与存储source和sink的连接器dataStream实例
         * */
        Map<String, DataStreamSource<String>> kafkaSources = new HashMap<>();
        for (String item : topic_list
        ) {
            //生成kafkaSource实例
            final KafkaSource<String> kafkaSource = kafkaSourceMap.get_source("kafka_source->".concat(item)).build();
            /*
             * 生成对应数据流：
             * mysqlSource->kafkaSink
             * kafkaSource->hiveSink
             * */
            final DataStreamSource<String> kafkaSource_dataStream;
            kafkaSource_dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source->".concat(item));
            /*
             * 存储当前数据流
             * */
            kafkaSources.put("kafka_source->".concat(item), kafkaSource_dataStream);
        }


        /*
         * transform
         * 转换对应的数据流至所需的形态
         * 并且分割流为入库流和错误流
         * 对应操作：map()/filter()/flatmap()等
         * */
        Map<String, SingleOutputStreamOperator> TransformedSources = new HashMap<>();
        for (String Topic_item : topic_list
        ) {
            final SingleOutputStreamOperator tmp_dataStream;
            for (String map_item : FlinkETL_MapperEnum.getEnumTypes()
            ) {
                if (Topic_item.equals(sink_database.concat("_").concat(map_item))) {
                    tmp_dataStream
                            = kafkaSources.get("kafka_source->".concat(Topic_item))
                            .filter(
                                    FlinkETL_FilterEnum.valueOf(map_item.concat("_filter")).getFilterFunClass()//载入filterFunction
                            ).map(
                                    FlinkETL_MapperEnum.valueOf(map_item.concat("_mapper")).getMapFunClass()//载入mapFunction
                            );
                    TransformedSources.put("kafka_source->".concat(Topic_item), tmp_dataStream);
                    break;
                }
            }
        }


        /*
         * 编写表格对应的schema
         * */
        final Schema omp_r_equipment_algo_ability_Schema =
                new omp_r_equipment_algo_ability()
                        .getLocalSchemaBuilder()
                        .build();


        /*
         * 生成hive表环境并进行相关配置工作
         * */
        final StreamTableEnvironment tableEnv = hiveCatalogMap.create_tableEnv(env, CATALOG_NAME);
        tableEnv.useCatalog(CATALOG_NAME);

        /*
         * 利用datastream和schema
         * 在tableEnv（表环境）中创建映射view
         * */
        String viewName = CATALOG_NAME.concat(".").concat(sink_database).concat(".").concat("tmp_omp_r_equipment_algo_ability");
        SingleOutputStreamOperator<omp_r_equipment_algo_ability> view_DataStream;
        view_DataStream = TransformedSources.get("kafka_source->".concat("ods_icity_omp_r_equipment_algo_ability"));
        tableEnv.createTemporaryView(
                viewName,
                view_DataStream,
                omp_r_equipment_algo_ability_Schema);//用流生成临时表
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);//切换hive方言
        tableEnv.executeSql("CREATE TABLE if not exists hiveCatalog.ljm_test.ods_icity_omp_r_equipment_algo_ability(\n" +
                "  id string, \n" +
                "  equipment_id string, \n" +
                "  ability_code string, \n" +
                "  belong_dept_id string, \n" +
                "  belong_lessee_id string \n" +
                ") \n" +
                "partitioned by(`DAY` STRING)\n" +
                "STORED AS parquet TBLPROPERTIES(\n" +
                //小文件自动合并，1.12版的新特性，解决了实时写hive产生的小文件问题
                "'auto-compaction'='true',\n" +
                //合并后的最大文件大小
                "'compaction.file-size'='128MB',\n" +
                "'format' = 'parquet',\n" +
                //压缩方式
                "'parquet.compression'='GZIP',\n" +
                //如果每小时一个分区，这个参数可设置为1 h，这样意思就是说数据延后一小时写入hdfs，能够达到数据的真确性，如果分区是天，这个参数也没必要设置了，今天的数据明天才能写入，时效性太差
                "'sink.partition-commit.delay'='30 s',\n" +
                //metastore值是专门用于写入hive的，也需要指定success-file
                //这样检查点触发完数据写入磁盘后会创建_SUCCESS文件以及hive metastore上创建元数据，这样hive才能够对这些写入的数据可查
                "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        /*
         * 对接数据流与sink端，开始执行动作：
         * kafkaSource-><transform>->hiveSink
         * */
        tableEnv.createStatementSet()
                .addInsertSql("insert into hiveCatalog.ljm_test.ods_icity_omp_r_equipment_algo_ability " +
                        "select *,'2022-10-13' as `DAY` from hiveCatalog.ljm_test.tmp_omp_r_equipment_algo_ability")
                .execute();
        tableEnv.executeSql("select * from hiveCatalog.ljm_test.tmp_omp_r_equipment_algo_ability").print();

    }
}
