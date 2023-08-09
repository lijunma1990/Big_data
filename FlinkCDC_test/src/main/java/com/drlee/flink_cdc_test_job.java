package com.drlee;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;

public class flink_cdc_test_job {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建一个 HiveCatalog，并在表环境中注册
//        String name = "hive_catalog";
//        String defaultDatabase = "fwb_test";
//        String hiveConfDir = "/etc/hive/conf.cloudera.hive";
//        String hiveVersion = "2.3.9";
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir,hiveVersion);
//        tableEnv.registerCatalog("hive_catalog", hive);
//        tableEnv.useCatalog("hive_catalog");

        //3.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("create database fwb_test");
        tableEnv.executeSql("use fwb_test");
//        tableEnv.executeSql("show tables");
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT," +
                " name STRING," +
                " phone_num STRING," +
                " primary key (`id`) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '172.32.0.72'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'mysql'," +
                " 'database-name' = 'fwb_test'," +
                " 'table-name' = 'test_user_info'" +
                ")");
//        tableEnv.executeSql("insert into fwb_test.user_info values('00','daima0201','15312626883')").print();
        tableEnv.executeSql("show TABLES").print();
//        tableEnv.executeSql("select * from user_info");
//        env.execute();
    }
}