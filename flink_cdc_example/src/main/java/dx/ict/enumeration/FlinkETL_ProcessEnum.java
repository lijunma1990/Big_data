package dx.ict.enumeration;

import dx.ict.processes.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * @author 李二白
 * @date 2023/04/13
 */
public enum FlinkETL_ProcessEnum {
    user_info_process("user_info", new user_info_process());

    private final String TableName;
    private final ProcessFunction ProcessFunClass;

    FlinkETL_ProcessEnum(String table_name, ProcessFunction processFunClass) {
        TableName = table_name;
        ProcessFunClass = processFunClass;
    }

    public String getTableName() {
        return TableName;
    }

    public ProcessFunction getProcessFunClass() {
        return ProcessFunClass;
    }

    @Override
    public String toString() {
        return "FlinkETL_ProcessEnum{" +
                "TableName='" + TableName + '\'' +
                ", ProcessFunClass=" + ProcessFunClass +
                '}';
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */

        FlinkETL_ProcessEnum[] Enums = FlinkETL_ProcessEnum.values();
        String names = "";
        for (FlinkETL_ProcessEnum item : Enums
        ) {
            names = names.concat(item.getTableName()).concat(",");
        }
        return names.split(",");
    }
}
