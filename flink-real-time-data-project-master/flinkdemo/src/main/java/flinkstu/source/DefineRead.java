package flinkstu.source;

import flinkstu.bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author KevenHe
 * @create 2022/2/13 22:49
 */
//×Ô¶¨Òåsource
public class DefineRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Sensor> dataStream =
                env.addSource(new MySensorSource());
        dataStream.print();

        env.execute();
    }
    public static class MySensorSource implements SourceFunction<Sensor> {
        private boolean running=true;
        public void run(SourceContext<Sensor> ctx) throws Exception {
            Random random =  new Random();

            HashMap<String,Double> sensorTemp = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                sensorTemp.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }
            while(running){
                for (String sensorId :sensorTemp.keySet()){
                    Double newTemp = sensorTemp.get(sensorId)+random.nextGaussian();
                    sensorTemp.put(sensorId,newTemp);
                    ctx.collect(new Sensor(sensorId,System.currentTimeMillis(),newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        public void cancel() {

        }
    }
}
