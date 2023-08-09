package dx.ict.maps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/28 10:50
 * @Version 1.0
 */
public class new_data_map implements MapFunction {

    @Override
    public Object map(Object item) throws Exception {
        return this.toTargetObject(item);
    }

    private Tuple2<String, Row> toTargetObject(Object item) {
        Tuple2<String, Row> item_ = (Tuple2) item;
        String table_name = (String) ((Tuple2) item).f0;

        return item_;
    }
}
