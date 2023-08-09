package dx.ict.enumeration;

import dx.ict.maps.*;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public enum FlinkETL_MapperEnum {
    /*
     * 定义对数据流DataStream的map操作的枚举
     * */
    omp_r_equipment_algo_ability_mapper("omp_r_equipment_algo_ability", new omp_r_equipment_algo_ability_map()),
    user_info_mapper("user_info", new user_info_map());

    private final String tableName;
    private final MapFunction mapFunClass;

    FlinkETL_MapperEnum(String tableName, MapFunction mapFunClass) {
        this.tableName = tableName;
        this.mapFunClass = mapFunClass;
    }

    public String getTableName() {
        return tableName;
    }

    public MapFunction getMapFunClass() {
        return mapFunClass;
    }

    @Override
    public String toString() {
        return "FlinkETL_MapEnum{" +
                "table_name='" + tableName + '\'' +
                ", mapFunClass=" + mapFunClass +
                '}';
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */

        FlinkETL_MapperEnum[] Enums = FlinkETL_MapperEnum.values();
        String names = "";
        for (FlinkETL_MapperEnum item : Enums
        ) {
            names = names.concat(item.getTableName()).concat(",");
        }
        return names.split(",");
    }
}
