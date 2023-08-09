package dx.ict.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class HiveMetadata {
    public static Map<String, List<String>> getMetadata() {
        // MySQL连接相关参数
        String url = "jdbc:mysql://172.32.0.67:3306/hive";
        String user = "root";
        String password = "Ict@123;";
        // 初始化Map存储数据结构
        Map<String, List<String>> metadata = new HashMap<>();
        Connection connection = null;
        PreparedStatement pstmtDBAndTableNames = null;
        ResultSet rsDBAndTableNames = null;

        try {
            // 建立与MySQL的连接
            connection = DriverManager.getConnection(url, user, password);

            // 获取库名及其对应表名的查询
            String queryDBAndTableNames = "SELECT DBS.NAME AS DB_NAME, TBLS.TBL_NAME AS TBL_NAME FROM DBS JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID;";
            pstmtDBAndTableNames = connection.prepareStatement(queryDBAndTableNames);
            rsDBAndTableNames = pstmtDBAndTableNames.executeQuery();

            while (rsDBAndTableNames.next()) {
                String dbName = rsDBAndTableNames.getString("DB_NAME");
                String tableName = rsDBAndTableNames.getString("TBL_NAME");
                // 添加表名到对应的库名的列表中
                metadata.computeIfAbsent(dbName, k -> new ArrayList<>()).add(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rsDBAndTableNames != null) {
                try {
                    rsDBAndTableNames.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmtDBAndTableNames != null) {
                try {
                    pstmtDBAndTableNames.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // 返回封装好的metadata信息
        return metadata;
    }

    public static void main(String[] args) {
        Map<String, List<String>> metadata = getMetadata();
        // 输出封装好的metadata信息
        System.out.println(metadata);
    }
}


