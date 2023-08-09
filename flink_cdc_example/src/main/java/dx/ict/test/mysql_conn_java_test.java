package dx.ict.test;

//import com.sun.xml.internal.bind.v2.bytecode.ClassTailor;

import dx.ict.pojo.USER_Example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 此类主要检测jdbc 连接是否成功
 * create by xiax.xpu on @Date 2019/3/21 20:32
 */

public class mysql_conn_java_test {
    public static void main(String[] args) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://172.32.0.72:3306/ljm_test";
        String username = "root";
        String password = "mysql";
        Connection connection = null;
        Statement statement = null;

        try {
            //加载驱动
            Class.forName(driver);
            //创建连接
            connection = DriverManager.getConnection(url, username, password);
            //获取执行语句
            statement = connection.createStatement();
            //执行查询，获得结果集
            ResultSet resultSet = statement.executeQuery("select * from user_info");
            //处理结果集
            while (resultSet.next()) {
                USER_Example user = new USER_Example(
                        resultSet.getInt("id"),
                        resultSet.getString("name").trim(),
                        resultSet.getString("phone_number").trim());
                System.out.println(user);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭连接 释放资源
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                connection.close();
            }
        }

    }
}
