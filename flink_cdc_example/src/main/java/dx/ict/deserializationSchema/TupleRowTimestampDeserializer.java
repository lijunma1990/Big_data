package dx.ict.deserializationSchema;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/23 10:02
 * @Version 1.0
 */
public class TupleRowTimestampDeserializer implements DeserializationSchema<Tuple3<String, Row, String>> {

    @Override
    public Tuple3<String, Row, String> deserialize(byte[] bytes) throws IOException {
        // 解析 Kafka 消息，将表名、行和时间戳封装为一个元组返回
        String json = new String(bytes, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(json);
        String tableName = jsonObject.getString("tableName");
        Long timestamp = jsonObject.getLong("timestamp");
        JSONArray jsonArray = jsonObject.getJSONArray("row");
        Row row = Row.of(jsonArray.getString(0), jsonArray.getString(1), jsonArray.getString(2));
        return Tuple3.of(tableName, row, timestamp.toString());
    }

    @Override
    public boolean isEndOfStream(Tuple3<String, Row, String> tuple3) {
        return false;
    }

    @Override
    public TypeInformation<Tuple3<String, Row, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, Row, String>>() {
        });
    }
}
