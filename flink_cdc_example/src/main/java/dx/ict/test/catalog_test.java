package dx.ict.test;

import dx.ict.util.HiveCatalogMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author 李二白
 * @usage ods_to_kafka_example
 * @date 2022/03/22
 */

public class catalog_test {
    public static void main(String[] args) {
        /** usages of hive sql pipe job (with hive_catalog)
         * //execute with explicit sink
         * tableEnv.from("InputTable").insertInto("OutputTable").execute()
         * tableEnv.executeSql("INSERT INTO OutputTable SELECT * FROM InputTable")
         * <p>
         * tableEnv.createStatementSet()
         * .add(tableEnv.from("InputTable").insertInto("OutputTable"))
         * .add(tableEnv.from("InputTable").insertInto("OutputTable2"))
         * .execute()
         * <p>
         * tableEnv.createStatementSet()
         * .addInsertSql("INSERT INTO OutputTable SELECT * FROM InputTable")
         * .addInsertSql("INSERT INTO OutputTable2 SELECT * FROM InputTable")
         * .execute()
         * // execute with implicit local sink
         * tableEnv.from("InputTable").execute().print()
         * tableEnv.executeSql("SELECT * FROM InputTable").print()
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

//        env.addSource(new Source_Kafka().add_source(bootstrap_servers, topics, group_id, OffsetsInitializer.latest(), new SimpleStringSchema()));
        HiveCatalogMap hiveCatalogMap = new HiveCatalogMap();
        hiveCatalogMap.add_Catalog(
                "hive_catalog",
                "ljm_test",
                "/etc/hive/conf.cloudera.hive",
                "",
                "2.3.9"
        );
        StreamTableEnvironment hiveEnv = hiveCatalogMap.create_tableEnv(env, "hive_catalog");
        System.out.println(hiveEnv.getCurrentDatabase());
        System.out.println(hiveEnv.getCurrentCatalog());
        System.out.println(hiveEnv.getConfig());
        hiveEnv.executeSql("show databases;");


        System.out.println(hiveEnv.executeSql("use catalog hive_catalog;"));

//        System.out.println(hiveEnv.executeSql("select * from hive_catalog.ljm_test.ods_user_info;"));

        hiveCatalogMap.tree_catalog("hive-catalog");
//        try {
//            env.execute("ods->kafka:ljm_test.user_info");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
