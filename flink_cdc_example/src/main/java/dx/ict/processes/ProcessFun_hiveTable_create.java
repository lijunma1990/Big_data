package dx.ict.processes;

import dx.ict.util.HiveCatalogMap;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/29 15:14
 * @Version 1.0
 */
public class ProcessFun_hiveTable_create extends ProcessFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>> {
    String partitionName = null;
    String timestampPattern = null;
    String commitDelay = null;
    String database = null;
    private Connection connection;


    public ProcessFun_hiveTable_create(String partitionName, String timestampPattern, String commitDelay, String database) {
        this.partitionName = partitionName;
        this.timestampPattern = timestampPattern;
        this.commitDelay = commitDelay;
        this.database = database;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取 Hive JDBC 连接
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String database = "ods_ict_market";
        String url = "jdbc:hive2://172.32.0.67:10000/".concat(database);
        String user = "hive";
        String password = "Ict@123;";
        this.connection = DriverManager.getConnection(url, user, password);

    }

    @Override
    public void processElement(Tuple6<String, String, String, String, String, String> value, Context ctx, Collector<Tuple6<String, String, String, String, String, String>> out) throws Exception {

        String hive_table_name = value.f0;
        final String op = value.f1;
        String table_schema = value.f2;
        String table_name = value.f3;
        String hive_create_ddl_after = value.f4;
        String drop_ddl_before = value.f5;
        hive_create_ddl_after = hive_create_ddl_after.replace("${partitionName}", this.partitionName)
                .replace("${timestampPattern}", this.timestampPattern)
                .replace("${commitDelay}", this.commitDelay);
        Statement stmt = this.connection.createStatement();
        if (op.equals("DELETE")) {
            stmt.execute(drop_ddl_before);
            System.out.println("操作>>>>>>>>>>>>>>>>:" + drop_ddl_before);
        } else if (op.equals("UPDATE")) {
            stmt.execute(drop_ddl_before);
            System.out.println("操作>>>>>>>>>>>>>>>>:" + drop_ddl_before);
            stmt.execute(hive_create_ddl_after);
            System.out.println("操作>>>>>>>>>>>>>>>>:" + hive_create_ddl_after);
        } else {
            stmt.execute(hive_create_ddl_after);
            System.out.println("操作>>>>>>>>>>>>>>>>:" + hive_create_ddl_after);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        // 关闭 Hive JDBC 连接
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
