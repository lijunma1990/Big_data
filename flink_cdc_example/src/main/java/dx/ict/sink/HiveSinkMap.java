package dx.ict.sink;

import dx.ict.enumeration.kafkaSerializationSchemaEnum;
import dx.ict.util.TopicMap;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 李二白
 * @date 2023/03/27
 * @Description TODO
 */
public class HiveSinkMap implements Serializable {

    final private Map<String, Properties> KafkaSinks = new HashMap<>();

    public HiveSinkMap() {
        /*
         * initializer
         * 存放<sink名,sink配置>对应的配置，
         * 真正的sink只有在调用本实例的get方法的时候才会生成sink的生成器,
         * 生成器在build()后方可使用。*/
        final Properties defaultProperty = new Properties();
        defaultProperty.put("brokers", "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092");
        defaultProperty.put("topic", "default");
        defaultProperty.put("SerializationSchema", "SimpleStringSchema");
        this.KafkaSinks.put("default", defaultProperty);
    }

    public void remove_sink(String SinkName) {
        /*
         * 用此方法删除Kafka的sink配置
         * */
        this.KafkaSinks.remove(SinkName);
    }

    public void remove_sinks(String regex) {
        /*
         * 用此方法删除Kafkasinks符合正则的所有sink配置
         * */

        //编译生成正则pattern,并进行删除
        Pattern pattern = Pattern.compile(regex);
        for (Iterator<String> item = this.KafkaSinks.keySet().iterator(); item.hasNext(); ) {
            String item_ = item.next();
            Matcher matcher = pattern.matcher(item_);
            if (matcher.matches()) {
                item.remove();
                System.out.println("delete: " + item);
            } else {
                System.out.println("keep: " + item);
            }
        }
    }


    public void add_sink(String SinkName_header,
                         String brokers,
                         String topics,
                         String valueSerializationSchema,
                         String keySerializationSchema
    ) {
//        String[] topics_list = topics.split(",");
//        for (String topics : topics_list
//        ) {
        final Properties localProperty = new Properties();
        String current_SinkName = SinkName_header.concat("->").concat(topics);
        localProperty.put("brokers", brokers);
        localProperty.put("topic", topics);
        localProperty.put("valueSerializationSchema", valueSerializationSchema);
        localProperty.put("keySerializationSchema", keySerializationSchema);
        this.KafkaSinks.put(current_SinkName, localProperty);
//        }
    }


    public void add_sink(String SinkName_header,
                         String brokers,
                         String topics
    ) {
        String[] topics_list = topics.split(",");
        for (String item : topics_list
        ) {
            final Properties localProperty = new Properties();
            String current_SinkName = SinkName_header.concat("->").concat(item);
            localProperty.put("brokers", brokers);
            localProperty.put("topic", item);
            localProperty.put("valueSerializationSchema", "");
            localProperty.put("keySerializationSchema", "");
            this.KafkaSinks.put(current_SinkName, localProperty);
        }
    }

    public KafkaSinkBuilder get_sink(String SinkName) {
            /*
        DataStream<String> stream = ...

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic-name")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

    stream.sinkTo(sink);
    */
//        // 在sink到指定的topic之前，应该先查看topic是否存在，若不存在，则创建之
//        final List<String> topic_Map = new topicMap("ods_", this.KafkaSinks.get(SinkName)
//                .getProperty("brokers"))
//                .list_topic();
//        List<String> topics = this.KafkaSinks.get(SinkName).getProperty("topics").split("\,");
//        List<String> topic_list = new ArrayList<>();
//        topic_list.isEmpty();
        final TopicMap topicMap = new TopicMap(this.KafkaSinks.get(SinkName).getProperty("brokers"));
//        topicMap.List_All_Topic();
        if (topicMap.notExist(this.KafkaSinks.get(SinkName).getProperty("topics"))) {
            topicMap.addTopic(this.KafkaSinks.get(SinkName).getProperty("topics"));
        }

        if (this.KafkaSinks.containsKey(SinkName)) {
            KafkaSinkBuilder<String> Kafka_sink_builder = KafkaSink.<String>builder();
            //定义kafka的record序列化schema
            final KafkaRecordSerializationSchemaBuilder<String> kafkaRecordSerializationSchema_builder;
            kafkaRecordSerializationSchema_builder = this.get_KafkaRecordSerializationSchemaBuilder(SinkName);

            Kafka_sink_builder.setBootstrapServers(this.KafkaSinks.get(SinkName).getProperty("brokers"));
            Kafka_sink_builder.setRecordSerializer(
                    kafkaRecordSerializationSchema_builder.build()
            );
            Kafka_sink_builder.setTransactionalIdPrefix("ods_icity_");
            Kafka_sink_builder.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
            return Kafka_sink_builder;
        } else {
            System.out.println("Not found such SinkName .");
            return null;
        }
    }


    public KafkaRecordSerializationSchemaBuilder<String> get_KafkaRecordSerializationSchemaBuilder(String SinkName) {
        /**
         * 构建时需要提供 KafkaRecordSerializationSchema 来将输入数据转换为 Kafka 的 ProducerRecord。
         * Flink 提供了 schema 构建器 以提供一些通用的组件，
         * 例如消息键（key）/消息体（value）序列化、topic 选择、消息分区，
         * 同样也可以通过实现对应的接口来进行更丰富的控制。
         * */

        /**
         * KafkaRecordSerializationSchema.builder()
         *     .setTopicSelector((element) -> {<>自定义的topic选择逻辑</><your-topic-selection-logic>})
         *     .setValueSerializationSchema(new SimpleStringSchema())
         *     .setKeySerializationSchema(new SimpleStringSchema())
         *     .setPartitioner(new FlinkFixedPartitioner())
         *     .build();
         *     */
        KafkaRecordSerializationSchemaBuilder<String> KafkaRecordSerializationSchema_builder;
        KafkaRecordSerializationSchema_builder = KafkaRecordSerializationSchema.builder();

//        final TopicMap topicMap = new TopicMap(this.KafkaSinks.get(SinkName).getProperty("brokers"));
//        topicMap.List_All_Topic();
//        if (topicMap.notExist(this.KafkaSinks.get(SinkName).getProperty("topic"))) {
//            topicMap.addTopic(this.KafkaSinks.get(SinkName).getProperty("topic"));
//        }

        KafkaRecordSerializationSchema_builder
                .setTopic(this.KafkaSinks.get(SinkName).getProperty("topic"));


        for (String item : kafkaSerializationSchemaEnum.getEnumTypes()
        ) {
            if (item.equals(this.KafkaSinks.get(SinkName).getProperty("keySerializationSchema"))) {
                KafkaRecordSerializationSchema_builder.setValueSerializationSchema(
                        kafkaSerializationSchemaEnum.valueOf(item).getSerializationSchema()
                );
            }
        }
//            新用法使用枚举遍历
//        switch (this.KafkaSinks.get(SinkName).getProperty("valueSerializationSchema")) {
//            case "MyKafkaSerializationSchema":
//                KafkaRecordSerializationSchema_builder.setValueSerializationSchema(
//                        kafkaSerializationSchemaEnum.MyKafkaSerializationSchema.getSerializationSchema()
//                );
//                break;
//            //添加自定义或者通用配置选项
////            case"xxx":
////                KafkaRecordSerializationSchema_builder.setValueSerializationSchema(new xxx());
////                break;
//            default:
//                KafkaRecordSerializationSchema_builder.setValueSerializationSchema(
//                        kafkaSerializationSchemaEnum.SimpleStringSchema.getSerializationSchema()
//                );
//                break;
//        }

        for (String item : kafkaSerializationSchemaEnum.getEnumTypes()
        ) {
            if (item.equals(this.KafkaSinks.get(SinkName).getProperty("keySerializationSchema"))) {
                KafkaRecordSerializationSchema_builder.setKeySerializationSchema(
                        kafkaSerializationSchemaEnum.valueOf(item).getSerializationSchema()
                );
            }
        }
//            新用法使用枚举遍历
//        switch (this.KafkaSinks.get(SinkName).getProperty("keySerializationSchema")) {
//            case "MyKafkaSerializationSchema":
//                KafkaRecordSerializationSchema_builder.setKeySerializationSchema(
//                        kafkaSerializationSchemaEnum.MyKafkaSerializationSchema.getSerializationSchema()
//                );
//                break;
//            //添加自定义或者通用配置选项
////            case"xxx":
////                KafkaRecordSerializationSchema_builder.setKeySerializationSchema(new xxx());
////                break;
//            default:
//                KafkaRecordSerializationSchema_builder.setKeySerializationSchema(
//                        kafkaSerializationSchemaEnum.SimpleStringSchema.getSerializationSchema()
//                );
//                break;
//        }
        KafkaRecordSerializationSchema_builder.setPartitioner(new FlinkFixedPartitioner<>());
        return KafkaRecordSerializationSchema_builder;
    }
}
