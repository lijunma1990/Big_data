package flinkstu.transform;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author KevenHe
 * @create 2022/2/14 16:20
 */
public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream =
                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\api\\sensor.txt");

        DataStream<Sensor> dataStream = inputStream.map(new
           MapFunction<String, Sensor>() {
               public Sensor map(String value) throws Exception {
                   String[] filed = value.split(",");
                   return new Sensor(filed[0],new Long(filed[1]),new Double(filed[2]));
//                   String[] fileds = value.split(",");
//                   return new Sensor(new String(fileds[0]), new Long(fileds[1]), new
//                           Double(fileds[2]));
               }
           });

//        //分组
//        KeyedStream<Sensor, Tuple> keyedStream = dataStream.keyBy("id");
//        //Reduce聚合，取最大的温度值，以及当前最新的时间戳
//        SingleOutputStreamOperator<Sensor> resultStream = keyedStream.reduce((ReduceFunction<Sensor>) (value1, value2) -> new Sensor(value1.getId(), value2.getTimestamp(),
//                Math.max(value1.getTemperature(), value2.getTemperature())));

        dataStream.print();
        env.execute();
    }
}
