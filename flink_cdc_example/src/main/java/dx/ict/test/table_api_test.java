package dx.ict.test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * flink table api test
 * @author 李二白
 * @date 2022/03/21
 * */

public class table_api_test {

    public static void main(String[] args) {

        // 创建流处理和流表环境（create environments of both APIs）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        //将数据流转化为表（ interpret the insert-only DataStream as a Table）
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

        //将表对象转化为view并排序（ register the Table object as a view and query it）
        //结果表以姓名分组，计算总分加值（the query contains an aggregation that produces updates）
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery(
                "SELECT name, SUM(score) FROM InputTable GROUP BY name");

        //将待上传表转化为changelog (interpret the updating Table as a changelog DataStream)
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // add a printing sink and execute in DataStream API
        resultStream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
