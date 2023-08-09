package flinkstu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author KevenHe
 * @create 2022/2/13 22:41
 */
public class KafkaRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop102:9092");
        prop.setProperty("group.id", "consumer-group");
        prop.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),prop));
        env.execute();
    }
}
