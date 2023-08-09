package flinkstu.source;

import flinkstu.bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author KevenHe
 * @create 2022/2/13 22:21
 */
//传感器温度读数的数据类型
public class CollectionRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Sensor> sensorDataStream = env.fromCollection(
                Arrays.asList(
                        new Sensor("sensor_1", 1547718199L, 35.8),
                        new Sensor("sensor_6", 1547718201L, 15.4),
                        new Sensor("sensor_7", 1547718202L, 6.7),
                        new Sensor("sensor_10", 1547718205L, 38.1)
                )
        );
        DataStream<Integer> integerDataStream =
                env.fromElements(1,2,4,67,189);

        sensorDataStream.print("data");
        integerDataStream.print("int").setParallelism(1);
        env.execute();
    }
}
