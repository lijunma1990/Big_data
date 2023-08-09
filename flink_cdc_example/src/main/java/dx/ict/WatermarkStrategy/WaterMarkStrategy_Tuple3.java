package dx.ict.WatermarkStrategy;

import dx.ict.deserializationSchema.TupleRowTimestampDeserializer;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;


public class WaterMarkStrategy_Tuple3 implements WatermarkStrategy<Tuple3<String, Row, String>> {
    @Override
    public WatermarkGenerator<Tuple3<String, Row, String>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator_Tuple3();
    }

    @Override
    public TimestampAssigner<Tuple3<String, Row, String>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner_Tuple3();
    }
}

