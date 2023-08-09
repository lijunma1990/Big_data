package dx.ict.deserializationSchema;

//import com.alibaba.fastjson.JSON;
//import com.ververica.cdc.connectors.shaded.org.apache.kafka.common.header.Headers;
//import com.ververica.cdc.connectors.shaded.org.apache.kafka.common.serialization.Deserializer;
//import com.ververica.cdc.connectors.shaded.org.apache.kafka.common.serialization.Serializer;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
//import java.io.Serializable;
//import java.util.Map;

/*
public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    private static String encoding = "UTF8";

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
       */
/* System.out.println("Record--partition::"+record.partition());
        System.out.println("Record--offset::"+record.offset());
        System.out.println("Record--timestamp::"+record.timestamp());
        System.out.println("Record--timestampType::"+record.timestampType());
        System.out.println("Record--checksum::"+record.checksum());
        System.out.println("Record--key::"+record.key());
        System.out.println("Record--value::"+record.value());*//*

        return new ConsumerRecord(record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                record.checksum(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                */
/*这里我没有进行空值判断，生产一定记得处理*//*

                new String(record.key(), encoding),
                new String(record.value(), encoding));
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }
}*/

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Types;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class MyKafkaDeserializationSchema_Tuple5 implements DeserializationSchema {


    // ------------------------------------------------------------------------
    //  Kafka Deserialization
    // ------------------------------------------------------------------------

    @Override
    public Tuple5<String, String, Row, String, String> deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer
                .wrap(message)
                .order(ByteOrder.LITTLE_ENDIAN);
        String bufferString = byteBuffertoString(buffer);
        return StringToTuple5(bufferString);
    }

    @Override
    public void deserialize(byte[] message, Collector out) throws IOException {
        ByteBuffer buffer = ByteBuffer
                .wrap(message)
                .order(ByteOrder.LITTLE_ENDIAN);
        String bufferString = byteBuffertoString(buffer);
        out.collect(StringToTuple5(bufferString));
    }

    /**
     * 将String转化为Tuple5
     */
    private Tuple5<String, String, Row, String, String> StringToTuple5(String bufferString) {
        final String[] StringSplit = bufferString.split(",");
        String f0 = StringSplit[0].replace("(", "");
        String f1 = StringSplit[1];
        String msg = "";
        for (int i = 2; i < StringSplit.length - 2; i++) {
            msg = msg.concat(StringSplit[i]).concat(",");
        }
        String Object = msg.substring(0, msg.length() - 1);
        final JSONObject jsonObject = JSONObject.parseObject(Object);
        final JSONObject index = JSONObject.parseObject(jsonObject.getString("index"));
        final JSONObject value = JSONObject.parseObject(jsonObject.getString("value"));
        final JSONObject field = JSONObject.parseObject(jsonObject.getString("field"));
        final Row f2 = new Row(index.size());
        for (int i = 0; i < index.size(); i++) {
            final String colName = index.getString(String.valueOf(i));
            final String fieldString = field.getString(colName);
            String[] split = fieldString.split("\\{");
            String schema_type = split[0];
            String data_type = split[1].substring(0, split[1].length() - 1);
            f2.setField(i, value.getString(colName));
        }
//        String kind = f1;
        switch (f1) {
            case "-D":
                f2.setKind(RowKind.DELETE);
                break;
            case "+U":
                f2.setKind(RowKind.UPDATE_AFTER);
                break;
            case "-U":
                f2.setKind(RowKind.UPDATE_BEFORE);
                break;
            default:
                f2.setKind(RowKind.INSERT);
                break;
        }
        String f3 = StringSplit[StringSplit.length - 2];
        String f4 = StringSplit[StringSplit.length - 1].substring(0, 1);
        return Tuple5.of(f0, f1, f2, f3, f4);
    }
//    private Tuple5<String, String, Row, String, String> StringToTuple5(String bufferString) {
//        final String[] StringSplit = bufferString.split(",");
//        String f0 = StringSplit[0].replace("(", "");
//        String f1 = StringSplit[1];
//        String msg = "";
//        for (int i = 2; i < StringSplit.length - 2; i++) {
//            msg = msg.concat(StringSplit[i]).concat(",");
//        }
//        String Object = msg.substring(0, msg.length() - 1);
//        final JSONObject jsonObject = JSONObject.parseObject(Object);
//        final JSONObject index = JSONObject.parseObject(jsonObject.getString("index"));
//        final JSONObject value = JSONObject.parseObject(jsonObject.getString("value"));
//        final JSONObject field = JSONObject.parseObject(jsonObject.getString("field"));
//        final Row f2 = new Row(index.size());
//        for (int i = 0; i < index.size(); i++) {
//            final String colName = index.getString(String.valueOf(i));
//            final String fieldString = field.getString(colName);
//            String[] split = fieldString.split("\\{");
//            String schema_type = split[0];
//            String data_type = split[1].substring(0, split[1].length() - 1);
//            if (data_type.equals("io.debezium.time.Timestamp:INT64")) {
//                data_type = "INT64";
//            }
//            final Schema.Type type = Schema.Type.valueOf(data_type);
//            switch (type) {
//                case STRING:
//                    f2.setField(i, value.getString(colName));
//                    break;
//                case MAP:
//                    f2.setField(i, value.getString(colName));
//                    break;
//                case ARRAY:
//                    f2.setField(i, value.getJSONArray(colName));
//                    break;
//                case BOOLEAN:
//                    f2.setField(i, value.getBoolean(colName));
//                    break;
//                case INT8:
//                    f2.setField(i, value.getInteger(colName));
//                    break;
//                case BYTES:
//                    f2.setField(i, value.getBytes(colName));
//                    break;
//                case INT16:
//                    f2.setField(i, value.getInteger(colName));
//                    break;
//                case STRUCT:
//                    f2.setField(i, value.getString(colName));
//                    break;
//                case INT32:
//                    f2.setField(i, value.getInteger(colName));
//                    break;
//                case INT64:
//                    f2.setField(i, value.getString(colName));
//                    break;
//                case FLOAT32:
//                    f2.setField(i, value.getFloat(colName));
//                    break;
//                case FLOAT64:
//                    f2.setField(i, value.getFloat(colName));
//                    break;
//                default:
//                    f2.setField(i, value.getString(colName));
//                    break;
//            }
//        }
//        switch (f1) {
//            case "-D":
//                f2.setKind(RowKind.DELETE);
//                break;
//            case "+U":
//                f2.setKind(RowKind.UPDATE_AFTER);
//                break;
//            case "-U":
//                f2.setKind(RowKind.UPDATE_BEFORE);
//                break;
//            default:
//                f2.setKind(RowKind.INSERT);
//                break;
//        }
//        String f3 = StringSplit[StringSplit.length - 2];
//        String f4 = StringSplit[StringSplit.length - 1].substring(0, 1);
//        return Tuple5.of(f0, f1, f2, f3, f4);
//    }

    /**
     * 将ByteBuffer类型转换为String类型
     */
    private static String byteBuffertoString(ByteBuffer buffer) {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {
            charset = StandardCharsets.UTF_8;
            decoder = charset.newDecoder();
            // charBuffer = decoder.decode(buffer);//用这个的话，只能输出来一次结果，第二次显示为空
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }

    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Tuple5<String, String, Row, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple5<String, String, Row, String, String>>() {
        });
    }
}
