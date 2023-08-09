package dx.ict.processes;

import dx.ict.pojo.user_info;
import dx.ict.pojo.user_info_E;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/28 11:25
 * @Version 1.0
 */
public class SideOutByTableName extends org.apache.flink.streaming.api.functions.ProcessFunction {

    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {

        //使用side output分流
        final OutputTag<Tuple2<String, Row>> mainStreamTag = new OutputTag<Tuple2<String, Row>>("main") {
        };
        final OutputTag<Row> TableStreamTag = new OutputTag<Row>("Table") {
        };

        Tuple2<String, Row> item = (Tuple2<String, Row>) value;
        String table_name = item.f0;

    }
}
