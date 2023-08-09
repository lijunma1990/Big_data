package dx.ict.jobs;

import com.alibaba.fastjson.JSONObject;
import dx.ict.WatermarkStrategy.MySqlSourceRecord_TimeAssigner;
import dx.ict.enumeration.*;
import dx.ict.pojo.user_info;
import dx.ict.pojo.user_info_E;
import dx.ict.source.KafkaSourceMap;
import dx.ict.util.HiveCatalogMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author 李二白
 * @date 2023/04/13
 */
public class FlinkETL_KafkaToHive_example {
    //    public static void main(String[] args)  {
////        Properties pros = new Properties();
////        pros.load(new FileReader("config.properties"));
//        String catalogName = "myhive";
//        String defaultDatabase = "temp";
//        String hiveConfDir = "/etc/hive/conf.cloudera.hive";
//        String hiveVersion = "2.3.9";
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        HiveCatalogEnv hiveCatalogEnv = new HiveCatalogEnv(env);
//        StreamTableEnvironment hiveEnv = hiveCatalogEnv.tableHiveEnv();
//
//
//        String kafka_omp_equipment_info = "insert into kafka.ods_icity_omp_equipment_info select * from mysql.ods_icity_omp_equipment_info";
//        String kafkaParse_omp_equipment_info = "insert into temp.ods_icity_omp_equipment_info select * from kafkaparse.ods_icity_omp_equipment_info";
//
////        hiveEnv.executeSql(kafka_omp_equipment_info);
//        hiveEnv.executeSql(kafkaParse_omp_equipment_info);
//    }
    public static void main(String[] args) {
        /*
         * 从kafka的topic订阅主题，并且将其存入hive中*/

        //创建Stream环境&&Stream表环境，配置hive_catalog
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
//        StreamTableEnvironment.create(env, settings);
        /**
         * @hive环境:
         * hive_properties.put(" catalogName ", " myhive ");
         * hive_properties.put("defaultDatabase", "kafka");
         * hive_properties.put("hiveConfDir", "/etc/hive/conf.cloudera.hive");
         * hive_properties.put("hiveVersion", "2.3.9");
         * */
        HiveCatalogMap hiveCatalogMap = new HiveCatalogMap();
        hiveCatalogMap.add_Catalog(
                "myhive",
                "ljm_test",
//                "/etc/hive/conf.cloudera.hive",
                "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.hive\\",
                "",
                "2.3.9"
        );
        final StreamTableEnvironment myhive_env = hiveCatalogMap
                .create_tableEnv(env, "myhive");
        myhive_env.getConfig().setSqlDialect(SqlDialect.HIVE);

        //接入Kafka流
        final KafkaSourceMap kafkaSourceMap = new KafkaSourceMap();
        kafkaSourceMap.add_source(
                "mykafka",
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                "ljm_test_user_info",
                "",
                "Flink-ETL-test",
                "earliest",
                "MyKafkaDeserializationSchema_String"//default -> SimpleStringSchema
        );
        final KafkaSource<String> mykafka_source
                = kafkaSourceMap.get_source("mykafka->ljm_test_user_info").build();
        final DataStreamSource<String> kafka_dataStreamSource;
        kafka_dataStreamSource = env.fromSource(mykafka_source,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5L)),
                "mykafka->ljm_test_user_info");

//        final WatermarkStrategy<String> ts_ms_waterMark = WatermarkStrategy
//                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner(
//                        new MySqlSourceRecord_TimeAssigner("ts_ms")
//                );
//
//        kafka_dataStreamSource.assignTimestampsAndWatermarks(ts_ms_waterMark);

        //打印kafka流,example如下
        // {"before":null,
        // "after":{"id":4,"name":"sakura","phone_number":"66666666666"},
        // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"ljm_test","sequence":null,"table":"user_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        // "op":"r",
        // "ts_ms":1680056498303,
        // "transaction":null}
//        SingleOutputStreamOperator user_info_stream = kafka_dataStreamSource
//                .filter(
//                        FlinkETL_FilterEnum.valueOf("user_info_filter").getFilterFunClass()
//                )
//                .map(
//                        FlinkETL_MapperEnum.valueOf("user_info_mapper").getMapFunClass()
//                );
//        kafka_dataStreamSource.print();
        //使用side output分流
        final OutputTag<user_info> mainStreamTag = new OutputTag<user_info>("main") {
        };
        final OutputTag<user_info_E> errorStreamTag = new OutputTag<user_info_E>("error") {
        };

        final SingleOutputStreamOperator process_stream = kafka_dataStreamSource.process(
                FlinkETL_ProcessEnum.valueOf("user_info_process").getProcessFunClass()
        );
        final DataStream user_info_errorStream = process_stream.getSideOutput(errorStreamTag);
        final DataStream user_info_mainStream = process_stream.getSideOutput(mainStreamTag);

        //创建动态表
//        TableEnv.createTemporaryView(
//                "user_info",
//                user_info_stream,
//                $("id"),
//                $("name"),
//                $("phone_number")
//        );//老用法，尽量不要用
//        Schema user_info_Schema = Schema.newBuilder()
//                .column("id", DataTypes.INT())
//                .column("name", DataTypes.STRING())
//                .column("phone_number", DataTypes.STRING())
//                .column("ts_ms", DataTypes.STRING())
//                .build();

        Schema user_info_Schema = new user_info().getLocalSchemaBuilder().build();
        Schema user_info_E_Schema = new user_info_E().getLocalSchemaBuilder().build();
        myhive_env.createTemporaryView("myhive.ljm_test.user_info", user_info_mainStream, user_info_Schema);
        myhive_env.createTemporaryView("myhive.ljm_test.user_info_e", user_info_errorStream, user_info_E_Schema);

//        myhive_env.executeSql("select * from myhive.ljm_test.user_info").print();
//        myhive_env.executeSql("select * from myhive.ljm_test.user_info_e").print();
        //创建hive表(如果hive中该表不存在会自动在hive上创建，也可以提前在hive中建好该表，flinksql中就无需再执行建表SQL，因为用了hive的catalog，flinksql运行时会找到表)
        myhive_env.executeSql(
                HiveTableEnum.valueOf("ods_user_info").getCreateStatement()
        );
        myhive_env.executeSql(
                HiveTableEnum.valueOf("ods_user_info").getCreateStatement_E()
        );
        //写hive表
        myhive_env.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        myhive_env.createStatementSet()
                .addInsertSql("insert into myhive.ljm_test.ods_user_info " +
                        "select " +
                        "id, " +
                        "name, " +
                        "phone_number, " +
                        "pt ," +
                        "DELETE_MARK ," +
                        "pt_date " +
                        "from myhive.ljm_test.user_info")
                .addInsertSql("insert into myhive.ljm_test.ods_user_info_e " +
                        "select " +
                        "id, " +
                        "name, " +
                        "phone_number, " +
                        "ERROR_COLUMNS, " +
                        "ERROR_MESSAGE, " +
                        "pt ," +
                        "DELETE_MARK ," +
                        "pt_date " +
                        "from myhive.ljm_test.user_info_e")
                .execute();
        //打印
//        myhive_env.executeSql("select * from myhive.ljm_test.ods_user_info").print();
//        myhive_env.executeSql("MSCK REPAIR TABLE ljm_test.ods_user_info");
    }
}