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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class MyKafkaDebeziumDeserializer_Tuple4 implements DeserializationSchema {


    // ------------------------------------------------------------------------
    //  Kafka Deserialization
    // ------------------------------------------------------------------------

    @Override
    public Tuple4<String, Row, String, String> deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer
                .wrap(message)
                .order(ByteOrder.LITTLE_ENDIAN);
        String bufferString = byteBuffertoString(buffer);
        return StringToTuple4(bufferString);
    }

    @Override
    public void deserialize(byte[] message, Collector out) throws IOException {
        ByteBuffer buffer = ByteBuffer
                .wrap(message)
                .order(ByteOrder.LITTLE_ENDIAN);
        String bufferString = byteBuffertoString(buffer);
        out.collect(StringToTuple4(bufferString));
    }

    /**
     * 将String转化为Tuple4
     */
    private Tuple4<String, Row, String, String> StringToTuple4(String bufferString) {
        final String[] StringSplit = bufferString.split(",");
        String f0 = StringSplit[0].replace("(", "");
        String msg = "";
        for (int i = 1; i < StringSplit.length - 2; i++) {
            msg = msg.concat(StringSplit[i]).concat(",");
        }
        String[] row = msg.substring(3, msg.length() - 2).split(",");
        final Row f1 = new Row(row.length);
        for (int i = 0; i < row.length; i++) {
            f1.setField(i,row[i]);
        }
        String kind = msg.substring(0, 2);
        switch (kind) {
            case "-D":
                f1.setKind(RowKind.DELETE);
                break;
            case "+U":
                f1.setKind(RowKind.UPDATE_AFTER);
                break;
            case "-U":
                f1.setKind(RowKind.UPDATE_BEFORE);
                break;
            default:
                f1.setKind(RowKind.INSERT);
                break;
        }
        String f2 = StringSplit[StringSplit.length - 2];
        String f3 = StringSplit[StringSplit.length - 1].substring(0, 1);
        return Tuple4.of(f0, f1, f2, f3);
    }


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
    public TypeInformation<Tuple4<String, Row, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple4<String, Row, String, String>>() {
        });
    }
}
