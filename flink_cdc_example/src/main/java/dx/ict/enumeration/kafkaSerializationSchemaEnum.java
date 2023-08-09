package dx.ict.enumeration;

import org.apache.flink.api.common.serialization.SerializationSchema;
import dx.ict.serializationSchema.MyKafkaSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public enum kafkaSerializationSchemaEnum {
    /*
     * 定义kafka序列化器的枚举
     * */
    MyKafkaSerializationSchema("MyKafkaSerializationSchema", new MyKafkaSerializationSchema()),
    SimpleStringSchema("SimpleStringSchema", new SimpleStringSchema());
//    None("", new SimpleStringSchema());

    private final SerializationSchema<String> SerializationSchema;
    private final String name;

    kafkaSerializationSchemaEnum(String name, SerializationSchema<String> SerializationSchemaName) {
        this.SerializationSchema = SerializationSchemaName;
        this.name = name;
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举名称的列表
         * */
        String names = "";
        for (kafkaSerializationSchemaEnum item : kafkaSerializationSchemaEnum.values()
        ) {
            names = names.concat(item.getName()).concat(",");
        }
        return names.split(",");
    }

    @Override
    public String toString() {
        return "kafkaSerializationSchemaEnum{" +
                "SerializationSchema=" + SerializationSchema +
                ", name='" + name + '\'' +
                '}';
    }

    public String getName() {
        return this.name;
    }

    public SerializationSchema<String> getSerializationSchema() {

        return this.SerializationSchema;
    }

}
