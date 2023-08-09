package dx.ict.deserializationSchema;

import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/25 14:53
 * @Version 1.0
 * MySqlDeserialization的模板
 */
public interface MySqlDeserialization {

    public void init();

    public void setTableRowTypeMap(Map tableRowTypeMap);
}
