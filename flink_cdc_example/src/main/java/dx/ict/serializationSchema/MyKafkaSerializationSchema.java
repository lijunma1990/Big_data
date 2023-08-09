package dx.ict.serializationSchema;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.common.errors.SerializationException;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

/**
 * @author 李二白
 * @date 2023/04/06
 * 序列化器
 */
public class MyKafkaSerializationSchema implements SerializationSchema {

    @Override
    public byte[] serialize(Object data) {
        try {
            byte[] serializedData;
//            int stringSize;
            if (data == null) {
                serializedData = new byte[0];
//                stringSize = 0;
                return serializedData;

            } else {
                serializedData = data
                        .toString()
                        .getBytes(StandardCharsets.UTF_8);
//                stringSize = serializedData.length;
                return serializedData;
            }

        } catch (
                Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }

    }
}
