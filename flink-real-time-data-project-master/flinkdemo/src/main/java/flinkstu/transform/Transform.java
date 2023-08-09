package flinkstu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author KevenHe
 * @create 2022/2/14 11:04
 */
public class Transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream =
                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\api\\sensor.txt");

        // map. String转为长度输出
        DataStream<Integer> mapStream = dataStream.map(new MapFunction<String, Integer>() {
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // flatmap. 按，切分字段
        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String,
                String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for( String field: fields )
                    out.collect(field);
                }
        });

        //filter. 筛选sensor_1开头的id对应的数据
        DataStream<String> filterStream = dataStream.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        //打印输出
        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
