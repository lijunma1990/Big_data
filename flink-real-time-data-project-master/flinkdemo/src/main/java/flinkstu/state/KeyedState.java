package flinkstu.state;

import flinkstu.bean.Sensor;
import flinkstu.transform.RichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author KevenHe
 * @create 2022/2/15 22:15
 */
public class KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        //设置时间为eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<Sensor> dataStream = inputStream.map((MapFunction<String, Sensor>) value -> {
            String[] filed = value.split(",");
            return new Sensor(filed[0], new Long(filed[1]), new Double(filed[2]));

        });
        //定义一个有状态的map操作，统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream =
                dataStream.keyBy("id")
                .map(new MyKeyCountMapper());

        env.execute();
    }
        //自定义实现RichMapFunction
    public static class  MyKeyCountMapper extends RichMapFunction<Sensor,Integer>{
            private ValueState<Integer> keyCountState;

            //其他类型状态的声明
            private ListState<String> listState;
            private MapState<String,Double> mapState;
//            private ReducingState<Sensor> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                keyCountState = getRuntimeContext().getState(
                        new ValueStateDescriptor<Integer>("key-count",Integer.class));

                listState = getRuntimeContext().getListState(
                        new ListStateDescriptor<String>("my-list",String.class));
                mapState  =getRuntimeContext().getMapState(
                        new MapStateDescriptor<String, Double>("my_map",String.class,Double.class)
                );
//                reducingState = getRuntimeContext().getReducingState(
//                        new ReducingStateDescriptor<Sensor>("")
//                )
        }

            @Override
            public Integer map(Sensor sensor) throws Exception {
                //其他状态api调用
                //list state
                for(String str : listState.get()){
                    System.out.println(str);
                }
                listState.add("hello");
                //map state
                mapState.get("1");
                mapState.put("2",12.3);
                mapState.remove("2");
                //reducing state
//                reducingState.add("");
                mapState.clear(); //all
                Integer count = keyCountState.value();
                count++;
                keyCountState.update(count);
                return count;
            }
        }
}
