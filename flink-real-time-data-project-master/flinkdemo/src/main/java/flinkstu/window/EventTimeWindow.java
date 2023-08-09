package flinkstu.window;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author KevenHe
 * @create 2022/2/15 16:08
 */
public class EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
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
        //乱序数据设置时间戳和watermark
        dataStream.assignTimestampsAndWatermarks(new
                        BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Sensor element) {
                return element.getTimestamp()*1000; //s
            }
        });
        // 升序数据设置时间戳和watermark
        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Sensor>() {
            @Override
            public long extractAscendingTimestamp(Sensor element) {
                return element.getTimestamp()*1000;
            }
        });

        OutputTag<Sensor> lateStream = new OutputTag<>("late");
        //基于事件时间的开窗聚合，15s内温度最小值
        SingleOutputStreamOperator<Sensor> minStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateStream)
                .minBy("temperature");
//        minStream.print();

    }
}
