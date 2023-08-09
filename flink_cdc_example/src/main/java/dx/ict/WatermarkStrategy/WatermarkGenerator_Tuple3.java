package dx.ict.WatermarkStrategy;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/24 14:15
 * @Version 1.0
 */

/**
 * 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 * 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 */
public class WatermarkGenerator_Tuple3 implements WatermarkGenerator<Tuple3<String, Row, String>> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒

    private long currentMaxTimestamp;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     */
    @Override
    public void onEvent(Tuple3<String, Row, String> event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    /**
     * 周期性的调用，也许会生成新的 watermark，也许不会。
     *
     * <p>调用此方法生成 watermark 的间隔时间由 {@link ExecutionConfig#getAutoWatermarkInterval()} 决定。
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}