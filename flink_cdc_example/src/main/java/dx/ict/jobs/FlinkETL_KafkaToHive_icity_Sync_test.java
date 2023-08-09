package dx.ict.jobs;

import dx.ict.source.KafkaSourceMap;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.HiveCatalogMap;
import dx.ict.util.TopicMap;
import dx.ict.util.rowTypeInfoConverter;
import dx.ict.util.tableRowTypeMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Author 李二白
 * @Description TODO
 * @date 2023/04/04  9:54
 * @Version 1.0
 */
public class FlinkETL_KafkaToHive_icity_Sync_test {
    /**
     * flink cdc Mysql整库同步
     */


    private static final Logger log = LoggerFactory.getLogger(FlinkETL_MysqlToKafka_new_data_Sync.class);
    private static List<Tuple3<String, Row, String>> batch;
    private static String tableName;
    private static RowTypeInfo rowTypeInfo;

    public static void main(String[] args) {


        // mysql端连接信息
//        String ETL_META_database = "ETL_META";
//        String mysqlSourceHeader = "mysql_source";
        String userName = "root";
        String passWord = "mysql";
        String host = "172.32.0.72";
        String database = "icity";
        // 如果是整库，tableList = ".*"
        String tableList = ".*";
        int port = 3306;
//        String mysqlDeserializerSchema = "MySqlDebeziumDeserializer";

        /*
         * hiveSink 配置
         * */
        String CATALOG_NAME = "hiveCatalog";
        String hiveConfDir =
                "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.hive\\";
//                "/etc/hive/conf.cloudera.hive";
        String hadoopConfDir =
                "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.yarn\\";
//        "/etc/hadoop/conf.cloudera.yarn";
        String hiveVersion = "2.3.9";
//        String sink_database = "test";
        String hive_sink_database = "temp";

        /*
         * kafkaSource 配置
         * */
        String kafka_SourceName_header = "kafka_source";
        String brokers = "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092";
        String topics = "MysqlToKafka_Sync_icity";
        String topicPattern = "";//example:"^ods_icity_.*"
        String groupId = "Flink_ETL_Sync_icity";
        String offsets = "earliest";
        String kafkaDeserializerSchema = "MyKafkaDeserializationSchema_Tuple5";//default -> SimpleStringSchema;


        /*
         * 创建运行环境
         * */
        // TODO 1. 环境准备
        final int batchSize = 1000;// 定义窗口和批处理大小（按数量分）
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);//设置X的并行度

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //设置运行模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//设置自动的运行模式

        // 使用文件系统作为状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://172.32.0.67:9870/flink/");


        /*
         * 创建source和map配置实例
         * */
        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();//创建mysqlSource集:此处用于插入
        final HiveCatalogMap hiveCatalogMap = new HiveCatalogMap();
        hiveCatalogMap.add_Catalog(
                CATALOG_NAME,
                hive_sink_database,
                hiveConfDir,
                hadoopConfDir,
                hiveVersion);
//        final StreamTableEnvironment HivetableEnv = hiveCatalogMap.create_tableEnv(env, CATALOG_NAME);
//        HivetableEnv.useCatalog(CATALOG_NAME);


        // 注册同步的mysql库对应的catalog
        MySqlCatalog mysqlCatalog = new MySqlCatalog("mysql_catalog", database, userName, passWord, String.format("jdbc:mysql://%s:%d", host, port));
        List<String> tables = new ArrayList<>();
        //判断该数据库是否存在
        log.info("If database exists:{}", mysqlCatalog.databaseExists(database));
        //从mysqlCatalog获取RowTypeInfo
        final tableRowTypeMap tableRowTypeMap_O = new tableRowTypeMap(mysqlCatalog, tableList, database);
        final Map<String, RowType> tableRowTypeMap = tableRowTypeMap_O.getTableRowTypeMap();
        final Map<String, RowTypeInfo> tableTypeInformationMap = tableRowTypeMap_O.getTableTypeInformationMap();
        final List<String> primaryKeys = tableRowTypeMap_O.getPrimaryKeys();
        final List<String> tablesInDatabase = tableRowTypeMap_O.getTables();
        final Map<String, String[]> fieldNamesMap = tableRowTypeMap_O.getFieldNamesMap();
        final Map<String, DataType[]> tableDataTypesMap = tableRowTypeMap_O.getTableDataTypesMap();

        /*
         * 创建source和map配置实例
         * */
//        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();//创建mysqlSource集:此处用于插入
        String[] topic_list = topics.split(",");
        final KafkaSourceMap kafkaSourceMap = new KafkaSourceMap();//创建kafkaSource集
        final TopicMap topicMap = new TopicMap(
                brokers,
                "^MysqlToKafka_Sync_.*$"
        );//创建topic集


        /*
         * 向source和map配置实例中插入配置数据
         * */

        for (String item : topic_list
        ) {
            kafkaSourceMap.add_source(
                    kafka_SourceName_header,
                    brokers,
                    item,
                    topicPattern,
                    groupId,
                    offsets,
                    kafkaDeserializerSchema//default -> SimpleStringSchema
            );//插入kafka连接配置source
        }


        /*
         * 生成与存储source和sink的连接器dataStream实例
         * */
        Map<String, DataStreamSource<Tuple5<String, String, Row, String, String>>> kafkaSources = new HashMap<>();
        for (String item : topic_list
        ) {
            //生成kafkaSource实例
            final KafkaSource kafkaSource = kafkaSourceMap.get_source("kafka_source->".concat(item)).build();
            /*
             * 生成对应数据流：
             * mysqlSource->kafkaSink
             * kafkaSource->hiveSink
             * */
            final DataStreamSource<Tuple5<String, String, Row, String, String>> kafkaSource_dataStream;
            String dataStreamName = kafka_SourceName_header.concat("->").concat(item);
            kafkaSource_dataStream = env.fromSource(kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    dataStreamName);
            log.info("Create kafkaDataStream:{}", dataStreamName);
            /*
             * 存储当前数据流
             * */
            kafkaSources.put("kafka_source->".concat(item), kafkaSource_dataStream);
            log.info("Save kafkaDataStream:{}", dataStreamName);
        }

        /*
         * transform
         * 转换对应的数据流至所需的形态
         * 并且分割流为入库流和错误流(如果有必要进行数据清洗)
         * 对应操作：map()/filter()/flatmap()等
         * */
        for (String kafkaDataSourceName : kafkaSources.keySet()
        ) {
            final DataStreamSource<Tuple5<String, String, Row, String, String>> kafkaDataStreamSource;
            kafkaDataStreamSource = kafkaSources.get(kafkaDataSourceName);
            final SingleOutputStreamOperator<List<Tuple3<String, Row, String>>> kafkaWindowedStream;
            kafkaWindowedStream = kafkaDataStreamSource
                    .map(
                            new MapFunction<Tuple5<String, String, Row, String, String>, Tuple3<String, Row, String>>() {
                                @Override
                                public Tuple3<String, Row, String> map(Tuple5<String, String, Row, String, String> value) throws Exception {
                                    final int len = value.f2.getArity();
                                    String pt = value.f3;
                                    String DELETE_MARK = value.f4;
                                    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                                    DateTimeFormatter hourFormat = DateTimeFormatter.ofPattern("HH");
                                    DateTimeFormatter minuteFormat = DateTimeFormatter.ofPattern("mm");
                                    final LocalDateTime date = new Timestamp(Long.parseLong(String.valueOf(pt))).toLocalDateTime();
                                    String pt_date = dateFormat.format(date);
                                    String pt_hour = hourFormat.format(date);
                                    String pt_min = minuteFormat.format(date);
                                    Row rowOut = new Row(len + 3);
                                    for (int i = 0; i < len; i++) {
                                        rowOut.setField(i, value.f2.getField(i));
                                    }
                                    rowOut.setField(len, pt);
//                                        rowOut.setField("pt", pt);
                                    rowOut.setField(len + 1, DELETE_MARK);
//                                        rowOut.setField("delete_mark", DELETE_MARK);
                                    rowOut.setField(len + 2, pt_date);
//                                        rowOut.setField("pt_date", pt_date);
//                                    System.out.println(tableName);
//                                    System.out.println(value.f2);
                                    return Tuple3.of(value.f0, rowOut, pt);
                                }
                            }
                    )
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<Tuple3<String, Row, String>>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                    .withTimestampAssigner(
                                            new TimestampAssignerSupplier<Tuple3<String, Row, String>>() {
                                                @Override
                                                public SerializableTimestampAssigner<Tuple3<String, Row, String>> createTimestampAssigner(Context context) {
                                                    return new SerializableTimestampAssigner<Tuple3<String, Row, String>>() {
                                                        @Override
                                                        public long extractTimestamp(Tuple3<String, Row, String> element, long recordTimestamp) {
                                                            return Long.parseLong(element.f2);
                                                        }
                                                    };
                                                }
                                            }
                                    )
                    )
                    .windowAll(GlobalWindows.create())
                    .trigger(CountTrigger.of(batchSize))
                    .process(
                            new ProcessAllWindowFunction<Tuple3<String, Row, String>, List<Tuple3<String, Row, String>>, GlobalWindow>() {
                                @Override
                                public void process(Context context, Iterable<Tuple3<String, Row, String>> elements, Collector<List<Tuple3<String, Row, String>>> out) throws Exception {
                                    List<Tuple3<String, Row, String>> batch = new ArrayList<>();
                                    for (Tuple3<String, Row, String> item : elements
                                    ) {
                                        batch.add(item);
                                    }
                                    for (Map.Entry<String, RowTypeInfo> item : tableTypeInformationMap.entrySet()) {
                                        String tableName = item.getKey();
                                        RowTypeInfo rowTypeInfo = item.getValue();
                                        batchSink(batch,
                                                tableName,
                                                rowTypeInfo,
                                                hiveCatalogMap,
                                                CATALOG_NAME,
                                                database,
                                                hive_sink_database);
                                    }
                                    out.collect(batch);
                                }
                            }
                    );

            kafkaWindowedStream.print();
            try {
                env.execute("FlinkETL_KafkaToHive_icity_Sync");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static void batchSink(List<Tuple3<String, Row, String>> batch,
                                  String tableName,
                                  RowTypeInfo rowTypeInfo,
                                  HiveCatalogMap hiveCatalogMap,
                                  String CATALOG_NAME,
                                  String database,
                                  String hive_sink_database) {

        final RowTypeInfo newRowTypeInfo = new rowTypeInfoConverter().hiveTypeInfoGen(rowTypeInfo);
        final Schema newSchema = new rowTypeInfoConverter().SchemaGen(newRowTypeInfo);

        OutputTag<Row> insertTag = new OutputTag<Row>("insert", newRowTypeInfo) {
        };
        OutputTag<Row> deleteTag = new OutputTag<Row>("delete", newRowTypeInfo) {
        };
        OutputTag<Row> updateTag = new OutputTag<Row>("update", newRowTypeInfo) {
        };
        OutputTag<Row> update_beforeTag = new OutputTag<Row>("update_before", newRowTypeInfo) {
        };
        OutputTag<Row> update_afterTag = new OutputTag<Row>("update_after", newRowTypeInfo) {
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment HivetableEnv = hiveCatalogMap.create_tableEnv(env, CATALOG_NAME);
        HivetableEnv.useCatalog(CATALOG_NAME);

        final DataStreamSource<Tuple3<String, Row, String>> tuple3DataSource = env.fromCollection(batch);

        //筛选表名一致的数据流，并且将其map成
        final SingleOutputStreamOperator<Row> tmp_dataStream = tuple3DataSource
                .filter(
                        data -> data.f0.equals(tableName)
                )
                .process(
                        new ProcessFunction<Tuple3<String, Row, String>, Row>() {
                            @Override
                            public void processElement(Tuple3<String, Row, String> value, Context ctx, Collector<Row> out) throws Exception {
                                String changeType = value.f1.getKind().toString();
                                switch (changeType) {
                                    case "INSERT":
                                        ctx.output(insertTag, value.f1);
                                        ctx.output(updateTag, value.f1);
                                        break;
                                    case "DELETE":
                                        ctx.output(deleteTag, value.f1);
                                        ctx.output(updateTag, value.f1);
                                        break;
                                    case "UPDATE_BEFORE":
                                        ctx.output(update_beforeTag, value.f1);
                                        break;
                                    case "UPDATE_AFTER":
                                        ctx.output(update_afterTag, value.f1);
                                        ctx.output(updateTag, value.f1);
                                        break;
                                    default:
                                        break;
                                }
                                out.collect(value.f1);
                            }
                        }
                );

        HivetableEnv.useDatabase(hive_sink_database);
        final DataStream<Row> updateStream = tmp_dataStream.getSideOutput(updateTag);
        String Hive_Table_Name = "ods_".concat(database).concat("_").concat(tableName);
        String tmp_view_Name = Hive_Table_Name.concat("_").concat("view");
        HivetableEnv.createTemporaryView(tmp_view_Name, updateStream, newSchema);
        String Hive_Table_Path = "${CatalogName}.${HiveDatabase}.${tableName}";
        String Tmp_View_Path = "${CatalogName}.${HiveDatabase}.${tmp_view_Name}";
        Hive_Table_Path = Hive_Table_Path
                .replace("${CatalogName}", CATALOG_NAME)
                .replace("${HiveDatabase}", hive_sink_database)
                .replace("${tableName}", Hive_Table_Name);
        Tmp_View_Path = Tmp_View_Path
                .replace("${CatalogName}", CATALOG_NAME)
                .replace("${HiveDatabase}", hive_sink_database)
                .replace("${tmp_view_Name}", tmp_view_Name);

        HivetableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String STMP = "INSERT INTO TABLE ${Hive_Table_Path} SELECT * FROM ${Tmp_View_Path}";
        STMP = STMP
                .replace("${Hive_Table_Path}", Hive_Table_Path)
                .replace("${Tmp_View_Path}", Tmp_View_Path);
        //创建一个语句集用于统一执行job的sink任务
        StatementSet statementSet = HivetableEnv.createStatementSet();
        statementSet.addInsertSql(STMP);
        //执行语句集
        statementSet.execute();
    }
}






