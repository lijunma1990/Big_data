package dx.ict.filters;

import dx.ict.pojo.HiveTableClass;
import dx.ict.pojo.omp_r_equipment_algo_ability;
import dx.ict.pojo.user_info;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class omp_r_equipment_algo_ability_filter implements FilterFunction<String> {


    @Override
    public boolean filter(String value) throws Exception {
        return false;
    }

}
