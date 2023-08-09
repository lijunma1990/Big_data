package flinkstu.window;

import flinkstu.bean.Sensor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import scala.Int;

/**
 * @author KevenHe
 * @create 2022/2/15 11:07
 */
public class TimeWindow_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间为eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStream<String> inputStream =
//                env.readTextFile("C:\\Users\\winqihe\\Desktop\\DataWareHouseFlink\\flinkdemo\\src\\main\\java\\flinkstu\\transform\\sensor.txt");


        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<Sensor> dataStream = inputStream.map((MapFunction<String, Sensor>) value -> {
            String[] filed = value.split(",");
            return new Sensor(filed[0], new Long(filed[1]), new Double(filed[2]));

        });
        //1.增量聚合函数
        dataStream
                .keyBy("id")
//                .countWindow(15,2);
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
//                .window(EventTimeSessionWindows.withGap(Time.seconds(15)));
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<Sensor, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Sensor value, Integer integer) {
                        return integer+1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer+acc1;
                    }
                });

        //2.全窗口函数
        //ProcessFunction
        dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<Sensor, Integer, Tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow timeWindow, Iterable<Sensor> input,
                                      Collector<Integer> out) throws Exception {
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(count);
                    }
                });

        env.execute();
    }
}
