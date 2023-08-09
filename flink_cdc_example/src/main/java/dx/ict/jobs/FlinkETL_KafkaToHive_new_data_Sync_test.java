package dx.ict.jobs;

import dx.ict.source.KafkaSourceMap;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.HiveCatalogMap;
import dx.ict.util.TopicMap;
import dx.ict.util.rowTypeInfoConverter;
import dx.ict.util.tableRowTypeMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
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
public class FlinkETL_KafkaToHive_new_data_Sync_test {
    /**
     * flink cdc Mysql整库同步
     */


    private static final Logger log = LoggerFactory.getLogger(FlinkETL_MysqlToKafka_new_data_Sync.class);

    public static void main(String[] args) {


        // mysql端连接信息
//        String ETL_META_database = "ETL_META";
//        String mysqlSourceHeader = "mysql_source";
        String userName = "root";
        String passWord = "mysql";
        String host = "172.32.0.72";
        String database = "new_data";
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
        String topics = "MysqlToKafka_Sync_new_data";
        String topicPattern = "";//example:"^ods_icity_.*"
        String groupId = "Flink_ETL_Sync_new_data";
        String offsets = "earliest";
        String kafkaDeserializerSchema = "MyKafkaDeserializationSchema_Tuple5";//default -> SimpleStringSchema;


        /*
         * 创建运行环境
         * */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);//设置X的并行度
        env.enableCheckpointing(5000);//设置5s的checkpoint间隔
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//设置自动的运行模式
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//注册表环境



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
        final StreamTableEnvironment HivetableEnv = hiveCatalogMap.create_tableEnv(env, CATALOG_NAME);
        HivetableEnv.useCatalog(CATALOG_NAME);


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


        //创建一个语句集用于统一执行job的sink任务
        StatementSet statementSet = HivetableEnv.createStatementSet();

        /*
         * transform
         * 转换对应的数据流至所需的形态
         * 并且分割流为入库流和错误流(如果有必要进行数据清洗)
         * 对应操作：map()/filter()/flatmap()等
         * */
        for (String kafkaDataSourceName : kafkaSources.keySet()
        ) {
            final RowTypeInfo item = tableTypeInformationMap.get("imp_equipment_warn");
            String tableName = "imp_equipment_warn";
            RowTypeInfo rowTypeInfo = item;
//                TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
//                String[] fieldNames = rowTypeInfo.getFieldNames();
//                // 创建一个新的字段类型数组，包括旧的字段类型和新的字段类型
//                TypeInformation<?>[] newFieldTypes = Arrays.copyOf(fieldTypes, fieldTypes.length + 3);
//                newFieldTypes[fieldTypes.length] = TypeInformation.of(String.class);
//                newFieldTypes[fieldTypes.length + 1] = TypeInformation.of(int.class);
//                newFieldTypes[fieldTypes.length + 2] = TypeInformation.of(String.class);
//                // 创建一个新的字段名称数组，包括旧的字段名称和新的字段名称
//                final String[] newFieldNames = Arrays.copyOf(fieldNames, fieldNames.length + 3);
//                newFieldNames[fieldNames.length] = "pt";
//                newFieldNames[fieldNames.length + 1] = "delete_mark";
//                newFieldNames[fieldNames.length + 2] = "pt_date";
//                // 创建一个新的 RowTypeInfo 对象
//                final RowTypeInfo newRowTypeInfo = new RowTypeInfo(newFieldTypes, newFieldNames);
            final RowTypeInfo newRowTypeInfo = new rowTypeInfoConverter().hiveTypeInfoGen(rowTypeInfo);
            final Schema newSchema = new rowTypeInfoConverter().SchemaGen(newRowTypeInfo);
//            OutputTag<Row> insertTag = new OutputTag<Row>("insert", Types.ROW_NAMED(newRowTypeInfo.getFieldNames(), newRowTypeInfo.getFieldTypes())) {
//            };
//            OutputTag<Row> deleteTag = new OutputTag<Row>("delete", Types.ROW_NAMED(newRowTypeInfo.getFieldNames(), newRowTypeInfo.getFieldTypes())) {
//            };
//            OutputTag<Row> updateTag = new OutputTag<Row>("update", Types.ROW_NAMED(newRowTypeInfo.getFieldNames(), newRowTypeInfo.getFieldTypes())) {
//            };
//            OutputTag<Row> update_beforeTag = new OutputTag<Row>("update_before", Types.ROW_NAMED(newRowTypeInfo.getFieldNames(), newRowTypeInfo.getFieldTypes())) {
//            };
//            OutputTag<Row> update_afterTag = new OutputTag<Row>("update_after", Types.ROW_NAMED(newRowTypeInfo.getFieldNames(), newRowTypeInfo.getFieldTypes())) {
//            };
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


            //筛选表名一致的数据流，并且将其map成
            final SingleOutputStreamOperator<Row> tmp_dataStream = kafkaSources.get(kafkaDataSourceName)
                    .filter(
                            data -> data.f0.equals(tableName)
                    )
                    .map(
                            new MapFunction<Tuple5<String, String, Row, String, String>, Row>() {
                                @Override
                                public Row map(Tuple5<String, String, Row, String, String> value) throws Exception {
                                    final int len = value.f2.getArity();
                                    String pt = value.f3;
                                    int DELETE_MARK = Integer.valueOf(value.f4);
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
                                    return rowOut;
                                }
                            }
                    )
                    .process(
                            new ProcessFunction<Row, Row>() {
                                @Override
                                public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                                    String changeType = value.getKind().toString();
                                    switch (changeType) {
                                        case "INSERT":
                                            ctx.output(insertTag, value);
                                            ctx.output(updateTag, value);
                                            break;
                                        case "DELETE":
                                            ctx.output(deleteTag, value);
                                            ctx.output(updateTag, value);
                                            break;
                                        case "UPDATE_BEFORE":
                                            ctx.output(update_beforeTag, value);
                                            break;
                                        case "UPDATE_AFTER":
                                            ctx.output(update_afterTag, value);
                                            ctx.output(updateTag, value);
                                            break;
                                        default:
                                            break;
                                    }
                                    out.collect(value);
                                }
                            }
                    );

            HivetableEnv.useDatabase(hive_sink_database);
            final DataStream<Row> insertStream = tmp_dataStream.getSideOutput(insertTag);
            final DataStream<Row> updateStream = tmp_dataStream.getSideOutput(updateTag);
            final DataStream<Row> deleteStream = tmp_dataStream.getSideOutput(deleteTag);
//            updateStream.print();

            final Table updateTable = HivetableEnv.fromChangelogStream(updateStream, newSchema, ChangelogMode.insertOnly());
            HivetableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            HivetableEnv.useCatalog(CATALOG_NAME);
//            final Table tmp_table = HivetableEnv.fromChangelogStream(updateStream);
            String Hive_Table_Name = "ods_".concat(database).concat("_").concat(tableName);
            String tmp_view_Name = Hive_Table_Name.concat("_").concat("view");
            HivetableEnv.createTemporaryView(tmp_view_Name, updateTable);
            String Hive_Table_Path = "${CatalogName}.${HiveDatabase}.${tableName}";
            Hive_Table_Path = Hive_Table_Path
                    .replace("${CatalogName}", CATALOG_NAME)
                    .replace("${HiveDatabase}", hive_sink_database)
                    .replace("${tableName}", Hive_Table_Name);
//            HivetableEnv.executeSql("select * from hiveCatalog.temp.".concat(tmp_view_Name)).print();
            String STMP = "INSERT INTO TABLE ${Hive_Table_Path} SELECT * FROM ${CatalogName}.${HiveDatabase}.${tmp_view_Name}";
            STMP = STMP.replace("${Hive_Table_Path}", Hive_Table_Path)
                    .replace("${CatalogName}", CATALOG_NAME)
                    .replace("${HiveDatabase}", hive_sink_database)
                    .replace("${tmp_view_Name}", tmp_view_Name);
            statementSet.addInsertSql(STMP);
        }
        //执行语句集
        statementSet.execute();
    }

}


