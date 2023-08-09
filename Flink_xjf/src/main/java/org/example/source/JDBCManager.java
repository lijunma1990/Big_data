package org.example.source;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class JDBCManager implements Serializable {
    private String url = null;
    private String UserName = null;
    private String PassWord = null;
    private String Deriver = "com.mysql.cj.jdbc.Driver";
    private static JDBCManager manager;
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet result = null;

    private JDBCManager(String url, String userName, String passWord) {
        this.url = url;
        UserName = userName;
        PassWord = passWord;
    }

    // 创建单例对象
    public static synchronized JDBCManager CreateJDBCManager(String url, String userName, String passWord) {
        if (null == manager) {
            // 上锁，避免产生多个对象
            synchronized (JDBCManager.class) {
                return manager = new JDBCManager(url, userName, passWord);
            }
        }
        return manager;
    }

    /**
     * 注册jdbc的connection连接器
     */
    public void RegisterConnection() throws ClassNotFoundException, SQLException {
        Class.forName(Deriver);
        connection = DriverManager.getConnection(this.url, this.UserName, this.PassWord);
    }

    // 数据库数据的插入/更新（sql的参数值不写死）
    public int upsertBatch(String sql, Object[] params) throws SQLException {
        if (params == null) {
            System.out.println("prams为null");
        } else {
            for (int i = 0; i <= params.length; i++) {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setObject(i + 1, params[i]);
                preparedStatement.addBatch();
            }
        }
        return preparedStatement.executeBatch().length;
    }

    // 数据库数据的获取
    public HashMap<Integer, ArrayList<Object>> executeQuery(String sql,int num) throws SQLException {
        preparedStatement = connection.prepareStatement(sql);
        result = preparedStatement.executeQuery();

        HashMap<Integer, ArrayList<Object>> hashMap = new HashMap<Integer, ArrayList<Object>>();
        int j =1;
        ArrayList<Object> objects;

        while (result.next()){
            objects = new ArrayList<>();
            for (int i = 0; i < num ;i++){
                Object o = result.getObject(i+1);
                objects.add(o);
            }
            hashMap.put(j,objects);
            j++;
        }
        return hashMap;
    }

    public void close() throws Exception {
        //关闭资源
        if (null != result){result.close();}
        if (null != preparedStatement){preparedStatement.close();}
        if (null != connection){connection.close();}
    }

}
