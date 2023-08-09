package flinkstu.window;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.util.Times;

/**
 * @author KevenHe
 * @create 2022/2/15 11:44
 */
public class CountWindow_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataStream<String> inputStream =
//                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\transform\\sensor.txt");


        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<Sensor> dataStream = inputStream.map((MapFunction<String, Sensor>) value -> {
            String[] filed = value.split(",");
            return new Sensor(filed[0], new Long(filed[1]), new Double(filed[2]));

        });
        //开计数窗口测试
        dataStream
                .keyBy("id")
                .countWindow(10,2)
                .aggregate(new myAggregate());

        env.execute();

        //测输出流
        OutputTag<Sensor> outputTag = new OutputTag<>("late");
        SingleOutputStreamOperator<Sensor> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");
        sumStream.getSideOutput(outputTag);

    }
}

 class myAggregate implements AggregateFunction<Sensor, Tuple2<Double,Integer>, Double> {

     @Override
     public Tuple2<Double, Integer> createAccumulator() {
         return new Tuple2<>(0.0,0);
     }

     @Override
     public Tuple2<Double, Integer> add(Sensor value, Tuple2<Double, Integer> acc) {
         return new Tuple2<>(acc.f0+value.getTemperature(),acc.f1+1);
     }

     @Override
     public Double getResult(Tuple2<Double, Integer> acc) {
         return acc.f0/acc.f1;
     }

     @Override
     public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
         return new Tuple2<>(a.f0+b.f0,a.f1+b.f1);
     }
 }
