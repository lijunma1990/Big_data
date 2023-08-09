package dx.ict.WatermarkStrategy;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/24 14:30
 * @Version 1.0
 */
public class TimestampAssigner_Tuple3 implements TimestampAssigner<Tuple3<String, Row, String>> {
    @Override
    public long extractTimestamp(Tuple3<String, Row, String> element, long recordTimestamp) {
        return Long.parseLong(element.f2);
    }
}
