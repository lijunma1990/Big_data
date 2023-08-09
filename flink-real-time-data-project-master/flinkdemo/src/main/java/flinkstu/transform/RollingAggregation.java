package flinkstu.transform;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author KevenHe
 * @create 2022/2/14 11:50
 */
public class RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream =
                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\api\\sensor.txt");

        //转为Sensor类型
        DataStream<Sensor> sensorStream = dataStream.map((MapFunction<String, Sensor>) value -> {
            String[] fileds = value.split(",");
            return new Sensor(fileds[0], new Long(fileds[1]), new Double(fileds[2]));
        });

        //分组
//        KeyedStream<Sensor, Tuple> keyedStream = sensorStream.keyBy("id");

        KeyedStream<Sensor, String> keyStream = sensorStream.keyBy(Sensor::getId);

        DataStream<Sensor> resultStream = keyStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
