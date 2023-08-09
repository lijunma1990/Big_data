package dx.ict.source;

import dx.ict.enumeration.kafkaDeserializationSchemaEnum;
import dx.ict.util.TopicMap;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 李二白
 * @date 2023/03/27
 */

public class KafkaSourceMap implements Serializable {
    final private Map<String, Properties> KafkaSources = new HashMap<>();

    public KafkaSourceMap() {
        /*
         * initializer
         * 存放<source名,source配置>对应的配置，
         * 真正的source只有在调用本实例的get方法的时候才会生成source的生成器,
         * 生成器在build()后方可使用。
         * */
        final Properties defaultProperty = new Properties();
        defaultProperty.put("brokers", "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092");
        defaultProperty.put("topic", "default");
//        defaultProperty.put("topicPattern", "");
        defaultProperty.put("groupId", "default");
        defaultProperty.put("offsets", "earliest");
        defaultProperty.put("DeserializationSchema", "SimpleStringSchema");
        this.KafkaSources.put("default", defaultProperty);
    }

    public void add_source(String SourceName_header,
                           String brokers,
                           String topicPattern,
                           String groupId,
                           String offsets,
                           String deserializerSchema
    ) {
        this.add_source(
                SourceName_header,
                brokers,
                "",
                topicPattern,
                offsets,
                groupId,
                deserializerSchema
        );
    }

    public void add_source(String SourceName_header,
                           String brokers,
                           String topics,
                           String topicPattern,
                           String groupId,
                           String offsets,
                           String deserializerSchema
    ) {
        /*
         // 从消费者组提交的offset开始(Start from committed offset of the consuming group, without reset strategy)
         .setStartingOffsets(OffsetsInitializer.committedOffsets())
         // 从提交的offset开始，如果提交的offset不存在，则从最早的地方开始(Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist)
         .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
         // 从某timestamp开始的第一条记录开始(Start from the first record whose timestamp is greater than or equals a timestamp)
         .setStartingOffsets(OffsetsInitializer.timestamp(1592323200L))
         // 从最早的offset开始(Start from earliest offset)
         .setStartingOffsets(OffsetsInitializer.earliest())
         // 从最后的offset开始(Start from latest offset)
         .setStartingOffsets(OffsetsInitializer.latest())
         * */
        String current_SourceName;
        if (!topicPattern.equals("") && topics.equals("")) {
            current_SourceName = SourceName_header.concat("->").concat(topicPattern.toString());
        } else {
            current_SourceName = SourceName_header.concat("->").concat(topics);
        }
        Properties localProperty = new Properties();
        localProperty.put("brokers", brokers);
//                localProperty.put("topic", item);//从主题中订阅消息
        localProperty.put("topics", topics);//从主题列表中订阅消息
        localProperty.put("topicPattern", topicPattern);//从主题正则表达式匹配的所有主题订阅消息，例如：KafkaSource.builder().setTopicPattern("topic.*")
        localProperty.put("groupId", groupId);//消费者组ID
        localProperty.put("offsets", offsets);
        localProperty.put("DeserializationSchema", deserializerSchema);
        this.KafkaSources.put(current_SourceName, localProperty);

    }


    public void remove_source(String SourceName) {
        /*
         * 用此方法删除Kafka的source配置
         * */
        this.KafkaSources.remove(SourceName);
    }

    public KafkaSourceBuilder get_source(String SourceName) {
        /**
         * @usage: add and return kafka source for the usage of StreamExecutionEnvironment object
         * @example: String brokers = "172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092";
         *         KafkaSource<String> source = KafkaSource.<String>builder()
         *                 .setBootstrapServers(brokers)
         *                 .setTopics("flinkcdc-kafka-to-temp-ods_icity_omp_equipment_info")
         *                 .setGroupId("flinkcdc")
         *                 .setStartingOffsets(OffsetsInitializer.earliest())
         *                 .setValueOnlyDeserializer(new SimpleStringSchema())
         *                 .build();
         *         env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
         *
         *@Partition列表，订阅指定的Partition：
         * final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
         *         new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
         *         new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
         * KafkaSource.builder().setPartitions(partitionSet);
         *
         **/

        final TopicMap topicMap = new TopicMap(this.KafkaSources.get(SourceName).getProperty("brokers"));
        topicMap.List_All_Topic();
        String[] topicList_ = this.KafkaSources.get(SourceName).getProperty("topics").split(",");
        for (String item : topicList_
        ) {
            if (topicMap.notExist(item) && !item.equals("")) {
                topicMap.addTopic(item);
            }
        }

        if (this.KafkaSources.containsKey(SourceName)) {
            KafkaSourceBuilder<String> KafkaSource_Builder = KafkaSource.<String>builder()
                    .setBootstrapServers(this.KafkaSources.get(SourceName).getProperty("brokers"))
//                .setTopics(this.KafkaSources.get(SourceName).getProperty("topics"))
//                .setTopicPattern(Pattern.compile(this.KafkaSources.get(SourceName).getProperty("topicPattern")))
                    .setGroupId(this.KafkaSources.get(SourceName).getProperty("groupId"));
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema());


            if (!this.KafkaSources.get(SourceName).getProperty("topics").equals("")) {
                KafkaSource_Builder.setTopics(this.KafkaSources.get(SourceName).getProperty("topics"));
            }
            if (!this.KafkaSources.get(SourceName).getProperty("topicPattern").equals("")) {
                KafkaSource_Builder.setTopicPattern(Pattern.compile(this.KafkaSources.get(SourceName).getProperty("topicPattern")));
            }

            if (Pattern.matches("timestamp/(.*/)", this.KafkaSources.get(SourceName).getProperty("offsets"))) {
                Pattern timestamp_pattern = Pattern.compile("timestamp[(](?<timestamp>.*)[)]");
                //创建正则匹配表达式，匹配timestamp(?<timestamp>)，括号内为捕获组<timestamp>
                Matcher matcher_ = timestamp_pattern.matcher(this.KafkaSources.get(SourceName).getProperty("offsets"));
                final boolean b = matcher_.find();
                KafkaSource_Builder.setStartingOffsets(OffsetsInitializer.timestamp(Long.parseLong(matcher_.group("timestamp"))));
            } else {
                switch (this.KafkaSources.get(SourceName).getProperty("offsets")) {
                    case "committedOffsets":
                        KafkaSource_Builder.setStartingOffsets(OffsetsInitializer.committedOffsets());
                        break;
                    case "EARLIEST":
                        KafkaSource_Builder.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));
                        break;
                    case "latest":
                        KafkaSource_Builder.setStartingOffsets(OffsetsInitializer.latest());
                        break;
                    default://earliest
                        KafkaSource_Builder.setStartingOffsets(OffsetsInitializer.earliest());
                        break;
                }
            }

            for (String item : kafkaDeserializationSchemaEnum.getEnumTypes()
            ) {
                if (item.equals(this.KafkaSources.get(SourceName).getProperty("DeserializationSchema")))
                    KafkaSource_Builder.setValueOnlyDeserializer(
                            kafkaDeserializationSchemaEnum.valueOf(item).getDeserializationSchema()
                    );
            }
//            新用法使用枚举遍历
//            switch (this.KafkaSources.get(SourceName).getProperty("DeserializationSchema")) {
//                case "MyKafkaDeserializationSchema_String":
//                    KafkaSource_Builder.setValueOnlyDeserializer(
//                            kafkaDeserializationSchemaEnum.MyKafkaDeserializationSchema_String.getDeserializationSchema()
//                    );
//                    break;
//                default:
//                    KafkaSource_Builder.setValueOnlyDeserializer(
//                            kafkaDeserializationSchemaEnum.SimpleStringSchema.getDeserializationSchema()
//                    );
//                    break;
//            }

            return KafkaSource_Builder;
        } else {
            System.out.println("Not found such SourceName .");
            return null;
        }
    }

}