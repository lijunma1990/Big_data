package dx.ict.WatermarkStrategy;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class MySqlSourceRecord_TimeAssigner implements SerializableTimestampAssigner<String> {

    private final String timestampColumn;

    public MySqlSourceRecord_TimeAssigner(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    /**
     * 提取数据里的timestamp字段为时间戳
     * @param element event对象
     * @param recordTimestamp element 的当前内部时间戳，或者如果没有分配时间戳，则是一个负数
     * @return The new timestamp.
     */
    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        final JSONObject jsonObject = JSONObject.parseObject(element);
        recordTimestamp = jsonObject.getLong(this.timestampColumn);
        return recordTimestamp;
    }
}
