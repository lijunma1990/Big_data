package flinkstu.sink;

import flinkstu.bean.Sensor;
import flinkstu.transform.RichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author KevenHe
 * @create 2022/2/14 18:06
 */
public class JDBCSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream =
                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\transform\\sensor.txt");

        DataStream<Sensor> dataStream = inputStream.map((MapFunction<String, Sensor>) value -> {
            String[] filed = value.split(",");
            return new Sensor(filed[0], new Long(filed[1]), new Double(filed[2]));

        });

        dataStream.addSink(new MyJdbcSink());
        env.execute();
    }
}
    //自定义sink Functioon
    class MyJdbcSink extends RichSinkFunction<Sensor> {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;
        // open 主要是创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo?useSSL=false", "root", "010613");
            // 创建预编译器，有占位符，可传入参数
            insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
        }
        // 调用连接，执行 sql
        @Override
        public void invoke(Sensor value, Context context) throws Exception {
            // 执行更新语句，注意不要留 super
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            // 如果刚才 update 语句没有更新，那么插入
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }
        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }