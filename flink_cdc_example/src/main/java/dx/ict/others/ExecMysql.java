package dx.ict.others;

import dx.ict.util.ConfigLoader;
import lombok.SneakyThrows;
import org.junit.After;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class ExecMysql {
    public static Object getConnection;

    public static void main(String[] args) {
        String datasource = "fwb_test.";
        String sql = "select * from " + datasource + "biao1";
        getExecSql(sql);
    }

    //定义接受信息的属性
    private static String driver;
    private static String url;
    private static String user;
    private static String password;
    private static Connection conn ;
    private static PreparedStatement pstmt ;
    private static ResultSet rs ;

    //使用静态代码块，在类加载时就读取配置文件信息
    static {
        url = ConfigLoader.get("mysql.url");
        user =ConfigLoader.get("mysql.user");
        password = ConfigLoader.get("mysql.password");

    }

    @SneakyThrows
    public static void getExecSql(String sql) {
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        while (true) {
            assert rs != null;
            if (!rs.next()) break;
            System.out.println(rs.getString("id"));
        }
        rs.close();
        pstmt.close();
        conn.close();
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//        Class.forName(driver);//加载MySQL驱动
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    /*通用的查询方法*/
    public static ResultSet query(Connection conn, String sql, Object... args) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
        }
         rs = ps.executeQuery();
        return rs;
    }
    @SneakyThrows
    public static PreparedStatement query(Connection conn, String sql) throws SQLException {
        pstmt = conn.prepareStatement(sql);
        return pstmt;
    }

    /*通用的增删改方法*/
    public static int update(Connection conn, String sql, Object... args) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                pstmt.setObject(i + 1, args[i]);
            }
        }
        int column = pstmt.executeUpdate();
        return column;
    }

    /*关闭资源方法*/
    public static boolean closeResource(ResultSet resultSet, PreparedStatement ps, Connection conn) {
        boolean flag = true;
        try {
            if (resultSet != null)
                resultSet.close();
        } catch (SQLException throwables) {
            flag = false;
            throwables.printStackTrace();
        }

        try {
            if (ps != null)
                ps.close();
        } catch (SQLException throwables) {
            flag = false;
            throwables.printStackTrace();
        }

        try {
            if (conn != null)
                conn.close();
        } catch (SQLException throwables) {
            flag = false;
            throwables.printStackTrace();
        }

        return flag;
    }

    @After
    public void close() throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (pstmt != null) {
            pstmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}


