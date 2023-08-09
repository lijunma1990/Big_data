package org.drlee.mysqlTest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * FlinkSQL test
 * @author Drlee
 * @date 2023/03/20
 * */
public class flinkSqlCdcTest {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id STRING primary key," +
                " name STRING," +
                " phone_number STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'hostname' = '172.32.0.72'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
            " 'password' = 'mysql'," +
                " 'database-name' = 'ljm_test'," +
                " 'table-name' = 'user_info'" +
                ")");


        //3. 查询数据并转换为流输出
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();
        //4. 启动
        env.execute("FlinkSQLCDC");
    }
}
