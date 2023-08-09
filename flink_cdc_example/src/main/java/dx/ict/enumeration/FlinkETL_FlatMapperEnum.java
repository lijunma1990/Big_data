package dx.ict.enumeration;

import dx.ict.flatmaps.*;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * @author 李二白
 * @date 2023/04/13
 */
public enum FlinkETL_FlatMapperEnum {

    user_info_flatmapper("user_info", new user_info_flatmapper());

    private final String TableName;
    private final FlatMapFunction flatMapFunction;

    FlinkETL_FlatMapperEnum(String TableName, FlatMapFunction flatMapFunction) {
        this.TableName = TableName;
        this.flatMapFunction = flatMapFunction;
    }

    public String getTableName() {
        return TableName;
    }

    public FlatMapFunction getFlatMapFunction() {
        return flatMapFunction;
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */

        FlinkETL_FlatMapperEnum[] Enums = FlinkETL_FlatMapperEnum.values();
        String names = "";
        for (FlinkETL_FlatMapperEnum item : Enums
        ) {
            names = names.concat(item.getTableName()).concat(",");
        }
        return names.split(",");
    }

}
