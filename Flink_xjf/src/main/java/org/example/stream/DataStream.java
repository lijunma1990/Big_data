package org.example.stream;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.Student;
import org.example.sink.MysqlDataSink;
import org.example.source.AddMySQLSource;
import org.example.util.GetParameterFromProperties;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

public class DataStream {
    public static void main(String[] args) {
        //获取配置文件里的配置参数
        String propertiesFilePath = "src/main/resources/db.properties";
        HashMap<String, String> parameter = GetParameterFromProperties.getParameter(propertiesFilePath);
        String url = parameter.get("url");
        String userName = parameter.get("userName");
        String passWord = parameter.get("passWord");

        // 1、执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // tableEnv

        // 2、数据源 source
        // url -- 源数据库地址；userName -- 源数据库用户名；passWord -- 源数据库用户密码
        org.apache.flink.streaming.api.datastream.DataStreamSource<Student> streamSource = env
                .addSource(new AddMySQLSource(url,userName,passWord));

        // 3、数据处理
        // streamSource.print();

        // 4、数据输出sink，此处Mysql是同一个
        // sql语句
        String sql = "insert into xjf_mysql.student2 (id,stunumbers,name,age,classnum) values (?,?,?,?,?) on duplicate key update stunumbers = VALUES(stunumbers),`name` = VALUES(`name`),age = VALUES(age),classnum = VALUES(classnum)";
        // 参数
        JdbcStatementBuilder<Student> jdbcStatementBuilder = new JdbcStatementBuilder<Student>() {
            @Override
            public void accept(PreparedStatement pstmt, Student student) throws SQLException {
                pstmt.setInt(1, student.getId());
                pstmt.setString(2, student.getStuNumbers());
                pstmt.setString(3, student.getName());
                pstmt.setInt(4, student.getAge());
                pstmt.setString(5, student.getClassNum());
            }
        };
        // streamSource -- 数据流；url -- 端数据库地址；userName -- 端数据库用户名；passWord -- 端数据库用户密码
        // sql -- 数据库的操作语句；jdbcStatementBuilder -- 必须参数
        new MysqlDataSink<Student>(streamSource, url, userName, passWord).toSink(sql, jdbcStatementBuilder);

        // 5、运行环境
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
