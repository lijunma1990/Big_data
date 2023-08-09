package dx.ict.enumeration;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import dx.ict.deserializationSchema.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public enum kafkaDeserializationSchemaEnum {
    /*
     * 定义kafka反序列化器的枚举
     * */
    MyKafkaDeserializationSchema_String("MyKafkaDeserializationSchema_String", new MyKafkaDeserializationSchema_String()),
    MyKafkaDeserializationSchema_Tuple5("MyKafkaDeserializationSchema_Tuple5", new MyKafkaDeserializationSchema_Tuple5()),
    MyKafkaDebeziumDeserializer_Tuple4("MyKafkaDebeziumDeserializer_Tuple4", new MyKafkaDebeziumDeserializer_Tuple4()),
    SimpleStringSchema("SimpleStringSchema", new SimpleStringSchema());
//    None("", new SimpleStringSchema());

    private final DeserializationSchema deserializationSchema;
    private final String name;

    kafkaDeserializationSchemaEnum(String name, DeserializationSchema deserializationSchemaName) {
        this.name = name;
        this.deserializationSchema = deserializationSchemaName;
    }

    public String getName() {
        return this.name;
    }

    public DeserializationSchema<String> getDeserializationSchema() {
        return this.deserializationSchema;
    }

    @Override
    public String toString() {
        return "kafkaDeserializationSchemaEnum{" +
                "deserializationSchema=" + deserializationSchema +
                ", name='" + name + '\'' +
                '}';
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举名称的列表
         * */

        kafkaDeserializationSchemaEnum[] kafkaDeserializationSchemaEnums = kafkaDeserializationSchemaEnum.values();
        String names = "";
        for (kafkaDeserializationSchemaEnum item : kafkaDeserializationSchemaEnums
        ) {
            names = names.concat(item.getName()).concat(",");
        }
        return names.split(",");
    }

}

