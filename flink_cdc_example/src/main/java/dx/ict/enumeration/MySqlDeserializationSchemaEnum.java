package dx.ict.enumeration;

import dx.ict.deserializationSchema.*;

import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/8 16:01
 * @Version 1.0
 */
public enum MySqlDeserializationSchemaEnum {
    /*
     * 定义kafka反序列化器的枚举
     * */
    MySqlDeserializationSchema_String("MySqlDeserializationSchema_String", new MySqlDeserializationSchema_String()),
    MySqlDeserializationSchema_Tuple5("MySqlDeserializationSchema_Tuple5", new MySqlDeserializationSchema_Tuple5()),
    MySqlDeserializationSchema_Tuple2("MySqlDeserializationSchema_Tuple2", new MySqlDeserializationSchema_Tuple2()),
    MySqlDebeziumDeserializer_Tuple2("MySqlDebeziumDeserializer_Tuple2", new MySqlDebeziumDeserializer_Tuple2()),
    MySqlDebeziumDeserializer_Tuple4("MySqlDebeziumDeserializer_Tuple4", new MySqlDebeziumDeserializer_Tuple4());
//    JsonDebeziumDeserializationSchema("JsonDebeziumDeserializationSchema", new JsonDebeziumDeserializationSchema());
//    None("", new JsonDebeziumDeserializationSchema());

    private static Map tableRowTypeMap;
    private final MySqlDeserialization deserializationSchema;
    private final String name;

    MySqlDeserializationSchemaEnum(String name, MySqlDeserialization deserializationSchemaName) {
        this.name = name;
        this.deserializationSchema = deserializationSchemaName;
    }

    public String getName() {
        return this.name;
    }

    public MySqlDeserialization getDeserializationSchema() {
        return this.deserializationSchema;
    }

    @Override
    public String toString() {
        return "MysqlDeserializationSchemaEnum{" +
                "deserializationSchema=" + deserializationSchema +
                ", name='" + name + '\'' +
                '}';
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举名称的列表
         * */

        MySqlDeserializationSchemaEnum[] kafkaDeserializationSchemaEnums = MySqlDeserializationSchemaEnum.values();
        String names = "";
        for (MySqlDeserializationSchemaEnum item : kafkaDeserializationSchemaEnums
        ) {
            names = names.concat(item.getName()).concat(",");
        }
        return names.split(",");
    }

}

