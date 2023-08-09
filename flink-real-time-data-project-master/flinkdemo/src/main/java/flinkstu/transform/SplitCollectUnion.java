package flinkstu.transform;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author KevenHe
 * @create 2022/2/14 16:44
 */
public class SplitCollectUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream =
                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\api\\sensor.txt");

        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map((MapFunction<String, Sensor>) value -> {
            String[] filed = value.split(",");
            return new Sensor(filed[0],new Long(filed[1]),new Double(filed[2]));

        });

        //1.分流操作，30度分为两条流
        SplitStream<Sensor> splitStream = dataStream.split(new OutputSelector<Sensor>() {
            @Override
            public Iterable<String> select(Sensor value) {
                return (value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<Sensor> highStream = splitStream.select("high");
        DataStream<Sensor> lowStream = splitStream.select("low");
        DataStream<Sensor> allStream = splitStream.select("high","low");

//        highStream.print();
//        lowStream.print();
//        allStream.print();

        //2.合流 collect 高温流转为二元组类型，做一个与低温流连接合并输出一个状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream =
                highStream.map(new MapFunction<Sensor, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Sensor value) throws Exception {
                        return new Tuple2<>(value.getId(),value.getTemperature());
                    }
                });
        ConnectedStreams<Tuple2<String, Double>, Sensor> connectedStreams = warningStream.connect(lowStream);

        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, Sensor, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning!");
            }

            @Override
            public Object map2(Sensor value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

//        resultStream.print();

        //3. union
        DataStream<Sensor> unionStream = highStream.union(lowStream, allStream);

        unionStream.print();

        env.execute();
    }
}
