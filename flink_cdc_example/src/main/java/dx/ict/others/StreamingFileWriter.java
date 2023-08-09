//import config.GlobConfig;
//
//import com.alibaba.fastjson.JSONObject;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//import utils.CoverTypeToSql;
//
//import utils.SinkHive;
//
//import java.util.List;
//
//import java.util.Properties;
//
//import java.util.stream.Collectors;
//
///**
// * @author 暮雪
// * @title: KafkaToHive
// * @projectName mysql2hive
// * @description: flink -> kafka ==> flink stream ==> hive
// * @date 2021.1.21
// */
//
//public class KafkaToHive {
//
//    public static void main(String[] args) throws Exception {
//
//// 创建执行环境
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        GlobConfig.putTableData();
//
////读取kafka数据
//
//        Properties properties = new Properties();
//
//        properties.setProperty("bootstrap.servers", GlobConfig.KAFKA_ADDR);
//
//        properties.setProperty("group.id", GlobConfig.KAFKA_CUSTOMER_GROUP);
//
//        DataStreamSource kafkaDS = env.addSource(
//
//                new FlinkKafkaConsumer(
//
//                        GlobConfig.KAFKA_TOPIC,
//
//                        new SimpleStringSchema(),
//
//                        properties)
//
//                        .setStartFromLatest()
//
//        );
//
//// 过滤掉无效数据
//
//        SingleOutputStreamOperator jsonData = kafkaDS.filter(json -> {
//
//            try {
//
//                JSONObject jsonStr = JSONObject.parseObject(json);
//
//                String mysqlName = jsonStr.getJSONObject("source").getString("table");
//
//                if (mysqlName == null || mysqlName.equals("") || GlobConfig.tableMap.get(mysqlName) == null) {
//
//                    return false;
//
//                }
//
//                return true;
//
//            } catch (Exception e) {
//
//                return false;
//
//            }
//
//        });
//
//        SingleOutputStreamOperator outputStreamOperator = jsonData.map(line -> {
//
//            JSONObject jsonObject = JSONObject.parseObject(line);
//
////数据来源的表名
//
//            String tableName = GlobConfig.tableMap.get(jsonObject.getJSONObject("source").getString("table"));
//
//            JSONObject afterData = jsonObject.getJSONObject("after");
//
//            List columnList = afterData.keySet().stream().collect(Collectors.toList());
//
////INSERT INTO table (c1,c2...) values (v1,v2..)
//
//            String sql = "INSERT INTO " + tableName + "( ";
//
//            String sql2 = " VALUES( ";
//
//            for (String column : columnList) {
//
//                sql += column + ", ";
//
//                sql2 += CoverTypeToSql.cover(afterData.getObject(column, Object.class)) + ", ";
//
//            }
//
//            String s1 = sql.substring(0, sql.length() - 2);
//
//            String s2 = sql2.substring(0, sql2.length() - 2);
//
//            String s3 = s1 + ") " + s2 + ")";
//
//            System.out.println(s3);
//
//            return s3;
//
//        });
//
////通过自定义sink 写到hive
//
//        outputStreamOperator.addSink(new SinkHive());
//
//        env.execute();
//
//    }
//
//}
//
//    GlobConfig.java 全局配置
//
//import java.util.HashMap;
//
///**
// * @author 暮雪
// * @title: GlobConfig
// * @projectName mysql2hive
// * @description: 配置类
// * @date 2021.1.20
// */
//
//public class GlobConfig {
//
////kafka 参数
//
//    public static String KAFKA_ADDR = "computer-28:9092,computer-29:9092,computer-30:9092";
//
//    public static String KAFKA_CUSTOMER_GROUP = "customer_muxue"; //可修改
//
//    public static String KAFKA_TOPIC = "sync_demo"; //需与数栖平台中建立的Kafka topic保持一致
//
////hive 连接信息
//
//    public static String HIVE_ADDR = "jdbc:hive2://computer-28:10000/data_collection";
//
//    public static String HIVE_USER = "admin";
//
//    public static String HIVE_PASSWORD = "";
//
////数据源 表名映射
//
//    public static HashMap tableMap = new HashMap<>();
//
//    /***
//     * 装载表对应关系
//     * tableMap.put("mysql_table_name","hive_table_name");
//
//     */
//
//    public static void putTableData() {
//
////示例对应表
//
//        tableMap.put("demo", "sync_demo");
//
////todo 在这里添加MySQL表与hive表的对应关系
//
//    }
//
//}
//
//    CoverTypeToSql.java 类型转换工具类
//
///**
// * @author 暮雪
// * @title: CoverTypeToSql
// * @projectName mysql2hive
// * @description: 不同数据类型转为sql
// * @date 2021.1.21
// */
//
//public class CoverTypeToSql {
//
//    public static String cover(Object value) {
//
//        if (value instanceof String || value instanceof Character) {
//
//            return "'" + value + "'";
//
//        } else {
//
//            return value + "";
//
//        }
//
//    }
//
//}
//
//    SinkHive.java 自定义sink类
//
//import config.GlobConfig;
//
//        import org.apache.flink.configuration.Configuration;
//
//        import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//        import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//        import java.sql.Connection;
//
//        import java.sql.DriverManager;
//
//        import java.sql.PreparedStatement;
//
///**
// * @author 暮雪
// * @title: SinkHive
// * @projectName mysql2hive
// * @description: 自定义sink写到hive表中
// * @date 2021.1.20
// */
//
//public class SinkHive extends RichSinkFunction implements SinkFunction {
//
//    private static String driverName = "org.apache.hive.jdbc.HiveDriver"; //驱动名称
//
//    private static String url = GlobConfig.HIVE_ADDR; //
//
//    private static String user = GlobConfig.HIVE_USER;
//
//    private static String password = GlobConfig.HIVE_PASSWORD;
//
//    private Connection connection;
//
//    private PreparedStatement statement;
//
//// 1,初始化
//
//    @Override
//
//    public void open(Configuration parameters) throws Exception {
//
//        super.open(parameters);
//
//        Class.forName(driverName);
//
//        connection = DriverManager.getConnection(url, user, password);
//
//    }
//
//// 2,执行
//
//    @Override
//
//    public void invoke(String value, Context context) throws Exception {
//
//        if (value != null && value != "") {
//
//            statement = connection.prepareStatement(value);
//
//            statement.execute();
//
//        }
//
//    }
//
//// 3,关闭
//
//    @Override
//
//    public void close() throws Exception {
//
//        super.close();
//
//        if (statement != null)
//
//            statement.close();
//
//        if (connection != null)
//
//            connection.close();
//
//    }
//
//}
