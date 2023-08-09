package dx.ict.jobs;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.processes.ProcessFun_hiveTable_create;
import dx.ict.source.MysqlSourceMap;
import dx.ict.util.HiveCatalogMap;
import dx.ict.util.tableRowTypeMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/28 14:29
 * @Version 1.0
 */
public class ETL_META_HIVETABLE_STRUCT_SYNC_ict_market {

    private static final Logger log = LoggerFactory.getLogger(FlinkETL_MysqlToKafka_new_data_Sync.class);

    public static void main(String[] args) {
        // mysql端连接信息
//        int stmpCount = 201;
        String ETL_META_database = "ETL_META";
        String mysqlSourceHeader = "mysql_source";
        String userName = "root";
        String passWord = "mysql";
        String host = "172.32.0.72";
        //进行同步的数据库名称，不在此列表中的库不同步
        String targetDatabaseList = "ict_market";
        // 如果是整库，tableList = ".*"
        String tableList = "ETL_META.ODS_TABLE_CREATE_DDL";
        int port = 3306;
        String mysqlDeserializerSchema = "MySqlDeserializationSchema_Tuple2";

        /*
         * hiveSink 配置
         * */
        String CATALOG_NAME = "hiveCatalog";
//        集群配置：
//        String hiveConfDir = "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.hive\\";
        String hiveConfDir = "/etc/hive/conf.cloudera.hive";
//        String hadoopConfDir = "D:\\Idea_projects\\flink_cdc_example\\conf.cloudera.yarn\\";
        String hadoopConfDir = "/etc/hadoop/conf.cloudera.yarn";
        String hiveVersion = "2.3.9";
        //Hive中对应的库名（存放同步的表）
        String sink_database = "ods_ict_market";
        String timestampPattern = "$pt_date 00:00:00";
        String commitDelay = "30 s";
        String partitionName = "`pt_date` string";

        /*
         * 创建运行环境
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置X的并行度
        env.enableCheckpointing(5000);//设置5s的checkpoint间隔
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//设置自动的运行模式
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);//注册表环境

        // 注册同步的库对应的catalog
        MySqlCatalog mysqlCatalog = new MySqlCatalog("mysql_catalog", ETL_META_database, userName, passWord, String.format("jdbc:mysql://%s:%d", host, port));
        List<String> tables = new ArrayList<>();
        //判断该数据库是否存在
        log.info("If database exists:{}", mysqlCatalog.databaseExists(ETL_META_database));
        //从mysqlCatalog获取RowTypeInfo
        final tableRowTypeMap tableRowTypeMap_O = new tableRowTypeMap(mysqlCatalog, tableList, ETL_META_database);
        final Map<String, RowType> tableRowTypeMap = tableRowTypeMap_O.getTableRowTypeMap();
        final Map<String, RowTypeInfo> tableTypeInformationMap = tableRowTypeMap_O.getTableTypeInformationMap();


        /*
         * 创建source和map配置实例
         * */
        final MysqlSourceMap mysqlSourceMap = new MysqlSourceMap();//创建mysqlSource集:此处用于插入
        final HiveCatalogMap hiveCatalogMap = new HiveCatalogMap();
        hiveCatalogMap.add_Catalog(
                CATALOG_NAME,
                sink_database,
                hiveConfDir,
                hadoopConfDir,
                hiveVersion);
        final StreamTableEnvironment HivetableEnv = hiveCatalogMap.create_tableEnv(env, CATALOG_NAME);
        HivetableEnv.useCatalog(CATALOG_NAME);

        /*
         * 向source配置实例中插入配置数据
         * */
        mysqlSourceMap.add_source(
                mysqlSourceHeader,
                host,
                String.valueOf(port),
                ETL_META_database,
                tableList,
                userName,
                passWord,
                "",
                mysqlDeserializerSchema
        );//获得HIVE语句体用


        /*
         * 生成与存储source和sink的连接器dataStream实例
         * */
        String currentSourceName = mysqlSourceHeader.concat("->").concat(tableList);
        final MySqlSource mySqlSource = mysqlSourceMap.get_source(currentSourceName).build();
        final DataStreamSource<Tuple2<String, String>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), currentSourceName);
//        final ConcurrentMap<String, String> stmpMap = Maps.newConcurrentMap();
        for (String tableName : tableList.split(",")
        ) {
            final SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> tmpStream = dataStreamSource
                    .filter(
                            data -> "ETL_META.".concat(data.f0).equals(tableName)
                    )
                    .keyBy(data -> data.f0)
                    .map(
                            new MapFunction<Tuple2<String, String>, Tuple6<String, String, String, String, String, String>>() {
                                public Tuple6<String, String, String, String, String, String> map(Tuple2<String, String> data) throws Exception {
                                    final JSONObject result = new JSONObject();
                                    System.out.println(data.f1);
                                    final JSONObject JsonItem = JSONObject.parseObject(data.f1);
                                    final JSONObject after = (JSONObject) JsonItem.get("after");
                                    final JSONObject before = (JSONObject) JsonItem.get("before");
                                    final String op = (String) JsonItem.get("op");
                                    if (op.equals("DELETE")) {
                                        final String drop_ddl_after = "";
                                        String hive_create_ddl_after = "";
                                        final String drop_ddl_before = (String) before.get("DROP_DDL");
                                        String hive_create_ddl_before = before.getString("HIVE_CREATE_DDL").concat(before.getString("PartitionBody"));
                                        final String hive_table_name = (String) before.get("HIVE_TABLE_NAME");
                                        final String table_schema = (String) before.get("TABLE_SCHEMA");
                                        final String table_name = (String) before.get("TABLE_NAME");
                                        return Tuple6.of(hive_table_name, op, table_schema, table_name, hive_create_ddl_after, drop_ddl_before);
                                    } else if (op.equals("UPDATE")) {
                                        final String hive_table_name = (String) after.get("HIVE_TABLE_NAME");
                                        final String table_schema = (String) after.get("TABLE_SCHEMA");
                                        final String table_name = (String) after.get("TABLE_NAME");
                                        final String drop_ddl_after = (String) after.get("DROP_DDL");
                                        final String drop_ddl_before = (String) before.get("DROP_DDL");
                                        String hive_create_ddl_after = after.getString("HIVE_CREATE_DDL").concat(after.getString("PartitionBody"));
                                        String hive_create_ddl_before = before.getString("HIVE_CREATE_DDL").concat(before.getString("PartitionBody"));
                                        return Tuple6.of(hive_table_name, op, table_schema, table_name, hive_create_ddl_after, drop_ddl_before);
                                    } else {
                                        final String drop_ddl_after = (String) after.get("DROP_DDL");
                                        final String drop_ddl_before = "";
                                        String hive_create_ddl_after = after.getString("HIVE_CREATE_DDL").concat(after.getString("PartitionBody"));
                                        String hive_create_ddl_before = "";
                                        final String hive_table_name = (String) after.get("HIVE_TABLE_NAME");
                                        final String table_schema = (String) after.get("TABLE_SCHEMA");
                                        final String table_name = (String) after.get("TABLE_NAME");
                                        return Tuple6.of(hive_table_name, op, table_schema, table_name, hive_create_ddl_after, drop_ddl_before);
                                    }
//                                    final String drop_ddl_after = (String) after.get("DROP_DDL");
//                                    String hive_create_ddl_after = after.getString("HIVE_CREATE_DDL").concat(after.getString("PartitionBody"));
//                                    final String drop_ddl_before = (String) before.get("DROP_DDL");
//                                    String hive_create_ddl_before = before.getString("HIVE_CREATE_DDL").concat(before.getString("PartitionBody"));
//                                    final String hive_table_name = (String) after.get("HIVE_TABLE_NAME");
//                                    result.put("after", after);
//                                    stmpMap.put(hive_table_name, hive_create_ddl_after);
//                                    return Tuple6.of(hive_table_name, op, drop_ddl_after, hive_create_ddl_after, drop_ddl_before, hive_create_ddl_before);
                                }
                            }
//                            ,tableTypeInformationMap.get(tableName)
                    )
                    .filter(
                            data -> {
                                String[] database_list = targetDatabaseList.split(",");
                                for (String db_item : database_list
                                ) {
                                    if (data.f2.equals(db_item)) {
                                        return true;
                                    }
                                }
                                return false;
                            }
                    )
                    .process(new ProcessFun_hiveTable_create(partitionName, timestampPattern, commitDelay, sink_database));

            tmpStream.print();
            try {
                env.execute("ETL_META_HIVETABLE_STRUCT_SYNC_ict_market");
            } catch (Exception e) {
                e.printStackTrace();
            }
//            tmpStream.writeAsText("D:\\tst", FileSystem.WriteMode.OVERWRITE);
//            final IterativeStream<Tuple2<String, String>> iterativeStream = tmpStream.iterate();//此处不适合用迭代流

//            try {
//                final CloseableIterator<Tuple6<String, String, String, String, String, String>> collect_stmps = tmpStream.executeAndCollect("ETL_META_HIVETABLE_STRUCT_SYNC");
//                while (true) {
//                    if (collect_stmps.hasNext()) {
//                        Tuple6<String, String, String, String, String, String> stmp_item = collect_stmps.next();
////                    System.out.println(stmp_item);
//                        HivetableEnv.useDatabase(sink_database);
//                        String hive_table_name = stmp_item.f0;
//                        final String op = stmp_item.f1;
//                        String table_schema = stmp_item.f2;
//                        String table_name = stmp_item.f3;
//                        String hive_create_ddl_after = stmp_item.f4;
//                        String drop_ddl_before = stmp_item.f5;
//                        hive_create_ddl_after = hive_create_ddl_after.replace("${partitionName}", partitionName)
//                                .replace("${timestampPattern}", timestampPattern)
//                                .replace("${commitDelay}", commitDelay);
//                        HivetableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//                        if (op.equals("DELETE")) {
//                            HivetableEnv.executeSql(drop_ddl_before);
//                            System.out.println("操作>>>>>>>>>>>>>>>>:" + drop_ddl_before);
//                            log.info("操作>>>>>>>>>>>>>>>>:{}", drop_ddl_before);
//                        } else if (op.equals("UPDATE")) {
//                            HivetableEnv.executeSql(drop_ddl_before);
//                            System.out.println("操作>>>>>>>>>>>>>>>>:" + drop_ddl_before);
//                            log.info("操作>>>>>>>>>>>>>>>>:{}", drop_ddl_before);
//                            HivetableEnv.executeSql(hive_create_ddl_after);
//                            System.out.println("操作>>>>>>>>>>>>>>>>:" + hive_create_ddl_after);
//                            log.info("操作>>>>>>>>>>>>>>>>:{}", hive_create_ddl_after);
//                        } else {
//                            HivetableEnv.executeSql(hive_create_ddl_after);
//                            System.out.println("操作>>>>>>>>>>>>>>>>:" + hive_create_ddl_after);
//                            log.info("操作>>>>>>>>>>>>>>>>:{}", hive_create_ddl_after);
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }

    }
}

