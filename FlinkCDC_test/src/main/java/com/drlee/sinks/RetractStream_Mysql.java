package com.drlee.sinks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * Created by Drlee on 2023-03-16.
 * 定义一个写入到mysql的sink
 */

public class RetractStream_Mysql extends RichSinkFunction<Tuple2<Boolean, Row>> {

    private static final long serialVersionUID = -4443175430371919407L;
    PreparedStatement ps;
    private Connection connection;

    /**
     * \* open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     * <p>
     * \* @param parameters
     * \* @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
//关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * \* 每条数据的插入都要调用一次 invoke() 方法
     * <p>
     * \* @param context
     * \* @throws Exception
     */
    @Override
    public void invoke(Tuple2<Boolean, Row> userPvEntity, Context context) throws Exception {
        String sql = "INSERT INTO flinkcomponent(componentname,componentcount,componentsum) VALUES(?,?,?);";
        ps = this.connection.prepareStatement(sql);

        ps.setString(1, userPvEntity.f1.getField(0).toString());
        ps.setInt(2, Integer.parseInt(userPvEntity.f1.getField(1).toString()));
        ps.setInt(3, Integer.parseInt(userPvEntity.f1.getField(2).toString()));
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "root");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
