package dx.ict.test;

import dx.ict.util.ConfigLoader;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ExecHive {
    private static String driverName = ConfigLoader.get("hive.driverName");
    private static String url = ConfigLoader.get("hive.url");
    private static String user = ConfigLoader.get("hive.user");
    private static String password = ConfigLoader.get("hive.password");

    protected static HiveMetaStoreClient client;
    protected static HiveConf hiveConf;
    protected static Connection conn = null;
    protected static Statement stat = null;
    protected static ResultSet rs = null;

    static {

        try {
            hiveConf = new HiveConf();
            hiveConf.addResource("hive-site.xml");
            client = new HiveMetaStoreClient(hiveConf);
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // 获取数据库
    public static List<String> getAllDatabases() {
        List<String> allDatabases = null;
        try {
            allDatabases = client.getAllDatabases();
        } catch (TException e) {
            e.printStackTrace();
        }
        return allDatabases;
    }

    // 获取指定数据库所有表
    public static List<String> getTables(String dbName) {
        List<String> tables = null;
        try {
            tables = client.getAllTables(dbName);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        return tables;
    }

    // 获取指定表所有字段信息
    public static List<FieldSchema> showSchemas(String dbName, String tbName) {
        List<FieldSchema> schemas = null;
        try {
            Table table = client.getTable(dbName, tbName);
            schemas = table.getSd().getCols();
            if (table.isSetPartitionKeys()) {
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                for (FieldSchema key : partitionKeys) {
                    key.setComment("partition key");
                }
                schemas.addAll(partitionKeys);
            }
        } catch (TException e) {
            e.printStackTrace();
        }
        return schemas;
    }

    public static List<String> showPartitions(String dbName, String tbName) {
        List<String> pars = new ArrayList<>();
        try {
            String sql = "show partitions " + dbName + "." + tbName;
            System.out.println("Running: " + sql);
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                String p1 = rs.getString(1);
                pars.add(p1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pars;
    }

    public static List<String> execSQL(String sql) {
        List<String> pars = new ArrayList<>();
        try {
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                String p1 = rs.getString(1);
                pars.add(p1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pars;
    }

    public static void createSQL(String sql) {
        try {
            stat.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 释放资源
    @After
    public void destory() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stat != null) {
            stat.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}

