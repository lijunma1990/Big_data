package org.example.source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.pojo.Student;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*
* 自定义数据源，实时获取mysql的数据
* xjf
* 2022/11/01
* */
public class AddMySQLSource extends RichParallelSourceFunction<Student> {
    // 标识符，是否实时接收数据
    private boolean isRunning = true ;
    private Connection conn = null;
    private PreparedStatement pstmt = null;
    private ResultSet result = null ;
    private Integer whereId = 0 ;


    // MysqlURL
    private String URL = null;
    // 用户名
    private String UseName = null;
    // 密码
    private String PassWord = null;


    public AddMySQLSource(String URL, String UseName, String PassWord) {
        this.URL = URL;
        this.UseName = UseName;
        this.PassWord = PassWord;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        Properties properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("src/main/resources/db.properties"));
        String URL = properties.getProperty("source.jdbc.url");
        String userName = properties.getProperty("source.jdbc.username");
        String passWord = properties.getProperty("source.jdbc.password");

        // 加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 创建连接
        conn = DriverManager.getConnection(URL,userName,passWord);
        // 创建数据库操作对象
        pstmt = conn.prepareStatement("select * from xjf_mysql.student WHERE id > ?");
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning){
            // 执行查询
            pstmt.setInt(1,whereId);
            result = pstmt.executeQuery();
            // 遍历查询结果，收集数据
            while (result.next()){
               Integer id = result.getInt(1);
               String stuNumbers = result.getString(2);
               String name = result.getString(3);
               Integer age = result.getInt(4);
               String classNum = result.getString(5);
                // 输出

                ctx.collect(new Student(id,stuNumbers,name,age,classNum));
                //whereId = id
            }
            //每三秒休眠一次，既每三秒查询一次数据库
            TimeUnit.SECONDS.sleep(3);
        }
    }

    @Override
    public void close() throws Exception {
        //关闭资源
        if (null != result){result.close();}
        if (null != pstmt){pstmt.close();}
        if (null != conn){conn.close();}
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
