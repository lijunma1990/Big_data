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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class MyKafkaDeserializationSchema_String implements DeserializationSchema<String> {


    // ------------------------------------------------------------------------
    //  Kafka Deserialization
    // ------------------------------------------------------------------------

    @Override
    public String deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer
                .wrap(message)
                .order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffertoString(buffer);
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
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
