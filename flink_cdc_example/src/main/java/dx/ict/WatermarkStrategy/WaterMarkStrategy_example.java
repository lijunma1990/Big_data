package dx.ict.WatermarkStrategy;

import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class WaterMarkStrategy_example implements WatermarkStrategy {
    @Override
    public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return null;
    }

    @Override
    public TimestampAssigner createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return null;
    }

    @Override
    public WatermarkAlignmentParams getAlignmentParameters() {
        return null;
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(TimestampAssignerSupplier timestampAssigner) {
        return null;
    }

    @Override
    public WatermarkStrategy withTimestampAssigner(SerializableTimestampAssigner timestampAssigner) {
        return null;
    }

    @Override
    public WatermarkStrategy withIdleness(Duration idleTimeout) {
        return null;
    }

    @Override
    public WatermarkStrategy withWatermarkAlignment(String watermarkGroup, Duration maxAllowedWatermarkDrift) {
        return null;
    }

    @Override
    public WatermarkStrategy withWatermarkAlignment(String watermarkGroup, Duration maxAllowedWatermarkDrift, Duration updateInterval) {
        return null;
    }
}
