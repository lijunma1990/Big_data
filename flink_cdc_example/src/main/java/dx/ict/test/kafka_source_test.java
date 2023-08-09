package dx.ict.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dx.ict.pojo.user_info;
import dx.ict.source.KafkaSourceMap;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.table.api.*;


public class kafka_source_test {

    public static void main(String[] args) {


        //创建Stream环境&&Stream表环境，配置hive_catalog
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment TableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());


        //接入Kafka流
        final KafkaSourceMap kafkaSourceMap = new KafkaSourceMap();
        kafkaSourceMap.add_source(
                "mykafka",
                "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092",
                "ljm-test-user-info",
                "",
                "Flink-ETL-Test",
                "earliest",
                "MyKafkaDeserializationSchema_String"//default -> SimpleStringSchema
        );
        final KafkaSource<String> mykafka_source
                = kafkaSourceMap.get_source("mykafka->ljm-test-user-info").build();
        final DataStreamSource<String> dataStreamSource = env.fromSource(mykafka_source,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "mykafka->ljm-test-user-info");
        //打印kafka_dataStreamSource
//        dataStreamSource.print();

        DataStream<user_info> user_info_stream = dataStreamSource.map(
                new MapFunction<String, user_info>() {
                    @Override
                    public user_info map(String str_) throws Exception {
                        JSONObject json_item =
                                (JSONObject) JSONObject.parseObject(str_)
                                        .get("after");
                        int id = Integer.parseInt(json_item.getString("id"));
                        String name = json_item.getString("name");
                        String phone_number = json_item.getString("phone_number");
                        String pt = JSONObject.parseObject(str_).getString("ts_ms");
                        return new user_info(id, name, phone_number,pt,0);
                    }
                }
        ).returns(user_info.class);
        //打印输出user_info流
        user_info_stream.print();

        final DataStream<String> forward = dataStreamSource.forward();

//        TableEnv.createTemporaryView(
//                "user_info",
//                user_info_stream,
//                $("id"),
//                $("name"),
//                $("phone_number")
//        );//老用法，尽量不要用

        Schema user_info_Schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("phone_number", DataTypes.STRING())
                .build();
        TableEnv.createTemporaryView("user_info", user_info_stream, user_info_Schema);


//        TableEnv.createTemporaryView("user_info", user_info_stream);
        TableEnv.executeSql("select * from user_info").print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println(">>>>>>>>>>>>>>>>>>>>" + Kafka_dataStreamSource_collector72);
        //输出dataStream类型的kafka的source

        //打印kafka流,example如下
        // {"before":null,
        // "after":{"id":4,"name":"sakura","phone_number":"66666666666"},
        // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"ljm_test","sequence":null,"table":"user_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        // "op":"r",
        // "ts_ms":1680056498303,
        // "transaction":null}

//        DataStream<user_info> user_info_stream = kafka_dataStreamSource.map(
//                (MapFunction<String, user_info>) (String stream_item) -> {
//                    JSONObject json_item = JSONObject.parseObject(stream_item);
//                    JSONObject after = (JSONObject) json_item.get("after");
//                    return new user_info((Integer) after.get("id"), (String) after.get("name"), (String) after.get("phone_number"));
//                }).returns(user_info.class);

//        user_info_stream.print();
        //创建动态表
//        myhive_env.createTemporaryView("user_info", user_info_stream);
    }


}
