package dx.ict.enumeration;

import dx.ict.filters.*;
import dx.ict.pojo.HiveTableClass;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public enum FlinkETL_FilterEnum {
    /*
     * 定义对数据流DataStream的filter操作的枚举
     * */
    omp_r_equipment_algo_ability_filter("omp_r_equipment_algo_ability", new omp_r_equipment_algo_ability_filter()),
    user_info_filter("user_info", new user_info_filter());

    private final String tableName;
    private final FilterFunction filterFunClass;

    FlinkETL_FilterEnum(String tableName, FilterFunction filterFunClass) {
        this.tableName = tableName;
        this.filterFunClass = filterFunClass;
    }

    public String getTableName() {
        return tableName;
    }

    public FilterFunction getFilterFunClass() {
        return filterFunClass;
    }

    @Override
    public String toString() {
        return "FlinkETL_FilterEnum{" +
                "tableName='" + tableName + '\'' +
                ", filterFunClass=" + filterFunClass +
                '}';
    }


    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */
        FlinkETL_FilterEnum[] Enums = FlinkETL_FilterEnum.values();
        String names = "";
        for (FlinkETL_FilterEnum item : Enums
        ) {
            names = names.concat(item.getTableName()).concat(",");
        }
        return names.split(",");
    }

}
