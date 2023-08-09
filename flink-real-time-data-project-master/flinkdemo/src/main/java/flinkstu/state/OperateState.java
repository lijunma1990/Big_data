package flinkstu.state;

import flinkstu.bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.Collections;
import java.util.List;

/**
 * @author KevenHe
 * @create 2022/2/15 22:04
 */
public class OperateState {
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
        //定义一个有状态的map操作 统计当前分区个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());

        resultStream.print();

        env.execute();
    }
    //自定义MapFunction
    public static class MyCountMapper implements MapFunction<Sensor,Integer>, ListCheckpointed<Integer> {
        //定义一个本地变量，作为算子状态
        //保存算子状态存盘checkpoint
        private Integer count = 0;
        @Override
        public Integer map(Sensor sensor) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num :state)
                count+=num;
        }
    }
}
