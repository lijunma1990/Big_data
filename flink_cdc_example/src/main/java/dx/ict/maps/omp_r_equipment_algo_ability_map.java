package dx.ict.maps;

import com.alibaba.fastjson.JSONObject;
import dx.ict.pojo.omp_r_equipment_algo_ability;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * map类
 * 用于对将kafkaSource流中的String实体（该实体来自于mysql的 changelog record）
 * 转化为
 * omp_r_equipment_algo_ability_map表实体
 * 配合新建的 schema 实体可以用于生成 view 来将数据存入hive中
 *
 * @author 李二白
 * @date 2023/04/06
 */
public class omp_r_equipment_algo_ability_map implements MapFunction<String, omp_r_equipment_algo_ability> {

    /*
     * 重写Map方法
     **/
    @Override
    public omp_r_equipment_algo_ability map(String kafka_item) {
        return this.toTargetObject(kafka_item);
    }

    private omp_r_equipment_algo_ability toTargetObject(String item) {
        JSONObject json_item =
                JSONObject.parseObject(item);
        final JSONObject after = (JSONObject) json_item.get("after");
        final JSONObject before = (JSONObject) json_item.get("before");

        String id;
        String equipment_id;
        String ability_code;
        String belong_dept_id;
        String belong_lessee_id;
        String pt;

        switch (json_item.getString("op")) {
            case "DELETE": {
                return getOmp_r_equipment_algo_ability(json_item, before);
            }
            default: {
                return getOmp_r_equipment_algo_ability(json_item, after);
            }
        }

        //        System.out.println(omp_r_equipment_algo_ability.toString());

    }

    private omp_r_equipment_algo_ability getOmp_r_equipment_algo_ability(JSONObject json_item, JSONObject JsonObject) {
        String id;
        String equipment_id;
        String ability_code;
        String belong_dept_id;
        String belong_lessee_id;
        String pt;
        int DELETE_MARK;
        id = JsonObject.getString("id");
        equipment_id = JsonObject.getString("equipment_id");
        ability_code = JsonObject.getString("ability_code");
        belong_dept_id = JsonObject.getString("belong_dept_id");
        belong_lessee_id = JsonObject.getString("belong_lessee_id");
        pt = json_item.getString("ts_ms");
        if (json_item.getString("op").equals("DELETE")) {
            DELETE_MARK = 1;
        } else {
            DELETE_MARK = 0;
        }
        return new omp_r_equipment_algo_ability(
                id,
                equipment_id,
                ability_code,
                belong_dept_id,
                belong_lessee_id,
                pt,
                DELETE_MARK
        );
    }
}
