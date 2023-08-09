package dx.ict.sink;

import dx.ict.util.ConfigLoader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/23 9:58
 * @Version 1.0
 */
public class HiveSink extends RichSinkFunction<Tuple3<String, Row, String>> {

    private Connection connection;
    private PreparedStatement statement;
    private int batchSize;
    private int count = 0;
    protected static HiveConf hiveConf;
    private transient HiveMetaStoreClient client;
    private static String driverName = ConfigLoader.get("hive.driverName");
    private static String url = ConfigLoader.get("hive.url");
    private static String user = ConfigLoader.get("hive.user");
    private static String password = ConfigLoader.get("hive.password");


    public HiveSink(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        HiveConf conf = new HiveConf();
//        conf.set(
//                "hive.metastore.uris",
//                "thrift://172.32.0.67:9083"
//        );
//        client = new HiveMetaStoreClient(conf);
        // 创建 Hive 连接和预编译语句
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public void invoke(Tuple3<String, Row, String> value, Context context) throws Exception {
        String table = value.f0;
        Row row = value.f1;
        StringBuilder VALUES = new StringBuilder();
        final int arity = row.getArity();
        String pt_date = row.getField(arity-1).toString();
        VALUES.append("?");
        for (int i = 1; i < arity-1; i++) {
            VALUES.append(",?");
        }
        String insertLine = "INSERT INTO ${TABLE_PATH} PARTITION (${PT_DATE}) VALUES (${VALUES})"
                .replace("${TABLE_PATH}", "ods_icity_".concat(table))
                .replace("${VALUES}", VALUES)
                .replace("${PT_DATE}", "pt_date='".concat(pt_date).concat("'"));

        statement = connection.prepareStatement(insertLine);
        // 根据表名替换预编译语句中的占位符
        statement.clearParameters();
        for (int i = 0; i < arity; i++) {
            statement.setString(i + 1, row.getField(i).toString());
        }
        statement.execute();
//        statement.addBatch();
//        count++;
//        if (count >= batchSize) {
//            statement.executeBatch();
//            count = 0;
//        }
    }

    @Override
    public void close() throws Exception {
        // 执行剩余的批处理语句并关闭连接
//        statement.executeBatch();
        statement.close();
        connection.close();
    }
}
