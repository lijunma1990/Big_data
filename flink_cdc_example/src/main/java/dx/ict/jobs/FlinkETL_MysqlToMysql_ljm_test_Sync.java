package dx.ict.jobs;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import dx.ict.deserializationSchema.MySqlDebeziumDeserializer_Tuple2;
import dx.ict.util.tableRowTypeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.tuple.Tuple2;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;


import java.util.List;
import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/18 10:54
 * @Version 1.0
 */
public class FlinkETL_MysqlToMysql_ljm_test_Sync {
    /**
     * flink cdc Mysql整库同步
     */


    private static final Logger log = LoggerFactory.getLogger(FlinkETL_MysqlToMysql_ljm_test_Sync.class);

    public static void main(String[] args) throws Exception {

        // source端连接信息
        String userName = "root";
        String passWord = "mysql";
        String host = "172.32.0.72";
        String db = "ljm_test";
        // 如果是整库，tableList = ".*"
        String tableList = ".*";
        int port = 3306;
        String MySqlDeserializationSchema = "MySqlDeserializationSchema_Tuple5";


        // sink连接信息模板
        String sink_url = "jdbc:mysql://172.32.0.72:3306/fwb_test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
        String sink_username = "root";
        String sink_password = "mysql";

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册同步的库对应的catalog
        MySqlCatalog mysqlCatalog = new MySqlCatalog("mysql_catalog", db, userName, passWord, String.format("jdbc:mysql://%s:%d", host, port));
        //判断该数据库是否存在
        log.info("If database exists:{}", mysqlCatalog.databaseExists(db));
        //从mysqlCatalog获取RowTypeInfo
        final tableRowTypeMap tableRowTypeMap_O = new tableRowTypeMap(mysqlCatalog, tableList, db);
        final Map<String, RowType> tableRowTypeMap = tableRowTypeMap_O.getTableRowTypeMap();
        final Map<String, RowTypeInfo> tableTypeInformationMap = tableRowTypeMap_O.getTableTypeInformationMap();
        final List<String> primaryKeys = tableRowTypeMap_O.getPrimaryKeys();
        final List<String> tables = tableRowTypeMap_O.getTables();
        final Map<String, String[]> fieldNamesMap = tableRowTypeMap_O.getFieldNamesMap();
        final Map<String, DataType[]> tableDataTypesMap = tableRowTypeMap_O.getTableDataTypesMap();

        for (String table : tables) {
            // 组装sink表ddl sql
            StringBuilder stmt = new StringBuilder();
            String tableName = table;
            String jdbcSinkTableName = String.format("jdbc_sink_%s", tableName);
            stmt.append("create table ").append(jdbcSinkTableName).append("(\n");
            String[] fieldNames = fieldNamesMap.get(table);
            final DataType[] fieldDataTypes = tableDataTypesMap.get(table);
            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t").append(column).append(" ").append(fieldDataType).append(",\n");
            }
            stmt.append(String.format("PRIMARY KEY (%s) NOT ENFORCED\n)", StringUtils.join(primaryKeys, ",")));
            String formatJdbcSinkWithBody = connectorWithBody
                    .replace("${tableName}", jdbcSinkTableName);
            String createSinkTableDdl = stmt.toString() + formatJdbcSinkWithBody;
            // 创建sink表
            log.info("createSinkTableDdl: {}", createSinkTableDdl);
            tEnv.executeSql(createSinkTableDdl);
        }

        // 监控mysql binlog
        MySqlSource mySqlSource = MySqlSource.<Tuple2<String, Row>>builder()
                .hostname(host)
                .port(port)
                .databaseList(db)
                .tableList(tableList)
                .username(userName)
                .password(passWord)
                .deserializer(new MySqlDebeziumDeserializer_Tuple2(tableRowTypeMap))
                .startupOptions(StartupOptions.initial())
                .build();
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_sync_icity").disableChaining();
        StatementSet statementSet = tEnv.createStatementSet();
        // dataStream转Table，创建临时视图，插入sink表
        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            //筛选表名一致的数据流，并且将其map成
            SingleOutputStreamOperator<Row> mapStream = dataStreamSource
                    .filter(data -> data.f0.equals(tableName))
                    .map(
                            new MapFunction<Tuple2<String, Row>, Row>() {
                                @Override
                                public Row map(Tuple2<String, Row> data) throws Exception {
                                    return data.f1;
                                }
                            },rowTypeInfo
//            (Tuple2<String, Row> data) -> {
//                                return data.f1;
//                            }, rowTypeInfo
                    );

            //打印rowTypeInfo的信息
            System.out.println(rowTypeInfo.toString());

            Table table = tEnv.fromChangelogStream(mapStream);
            String temporaryViewName = String.format("t_%s", tableName);
            tEnv.createTemporaryView(temporaryViewName, table);
//            tEnv.executeSql("select * from ".concat(temporaryViewName)).print();
            String sinkTableName = String.format("jdbc_sink_%s", tableName);
            String insertSql = String.format("insert into %s select * from %s", sinkTableName, temporaryViewName);
            log.info("add insertSql for {},sql: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();
    }
}
