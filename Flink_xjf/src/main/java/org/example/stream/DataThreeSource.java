package org.example.stream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.source.JDBCManager;
import org.example.util.GetParameterFromProperties;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class DataThreeSource {
    public static void main(String[] args) {
        //获取配置文件里的配置参数
        String propertiesFilePath = "src/main/resources/db.properties";
        HashMap<String, String> parameter = GetParameterFromProperties.getParameter(propertiesFilePath);
        String url = parameter.get("url");
        String userName = parameter.get("userName");
        String passWord = parameter.get("passWord");

        // 连接mysql获取数据
        StreamExecutionEnvironment env;
        //String sql = "select id,stunumbers,name,age,classnum from xjf_mysql.student";

        // stringBuffer 封装 sql 语句
        StringBuffer SQLBuffer = new StringBuffer();
        SQLBuffer.append("select ");
        SQLBuffer.append("id,stunumbers,name,age,classnum ");
        SQLBuffer.append("from ");
        SQLBuffer.append("xjf_mysql.student ");
        System.out.println(SQLBuffer);


        JDBCManager jdbcManager = JDBCManager.CreateJDBCManager(url, userName, passWord);
        try {
            ArrayList arrayListValue = new ArrayList();
            jdbcManager.RegisterConnection();
            HashMap<Integer, ArrayList<Object>> hashMap = jdbcManager.executeQuery(String.valueOf(SQLBuffer), 5);
            for (int i =1;i<= hashMap.size();i++){
                ArrayList<Object> arrayValue = hashMap.get(i);
                arrayListValue.add(arrayValue);
            }
            //source
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStreamSource<Object> objectDataStreamSource = env.fromCollection(arrayListValue);

            // 数据处理
            objectDataStreamSource.print();

            // sink

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // 环境启动
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
