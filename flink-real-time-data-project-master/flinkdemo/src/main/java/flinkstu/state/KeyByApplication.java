package flinkstu.state;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author KevenHe
 * @create 2022/2/15 23:00
 */
public class KeyByApplication {
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
        //定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warningStream = dataStream
                .keyBy("id")
                .flatMap(new TempChangeWarning(10.0));



        env.execute();
    }
    //自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<Sensor,
            Tuple3<String, Double, Double>>{
        //私有属性，温度跳变的阈值
        private Double threshold;
        //定义状态，保存上一次温度值
        private ValueState<Double> lastTempState;

        public TempChangeWarning(Double threshold) {
            this.threshold= threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState =getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last_temp",Double.class,Double.MIN_VALUE));
        }
        @Override
        public void flatMap(Sensor value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取上次温度值进行比较
            Double lastTemp = lastTempState.value();
            //如果状态不为null，那么就判断两次温度差
            if(lastTemp!=null){
                Double diff = Math.abs(value.getTemperature()-lastTemp);
                if (diff>=threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }
            //更新状态
            lastTempState.update(value.getTemperature());
        }
        @Override
        public void close() throws Exception{
            lastTempState.clear();
        }
    }
}