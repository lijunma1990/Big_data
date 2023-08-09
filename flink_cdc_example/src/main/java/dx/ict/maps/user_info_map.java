package dx.ict.maps;

import com.alibaba.fastjson.JSONObject;
import dx.ict.pojo.omp_r_equipment_algo_ability;
import dx.ict.pojo.user_info;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * map类
 * 用于对将kafkaSource流中的String实体（该实体来自于mysql的 changelog record）
 * 转化为
 * user_info表实体
 * 配合新建的 schema 实体可以用于生成 view 来将数据存入hive中
 *
 * @author 李二白
 * @date 2023/04/06
 */
public class user_info_map implements MapFunction<String, user_info> {

    /*
     * 重写Map方法
     **/
    @Override
    public user_info map(String kafka_item) {
        return this.toTargetObject(kafka_item);
    }

    private user_info toTargetObject(String item) {

        JSONObject json_item = JSONObject.parseObject(item);
        JSONObject after = (JSONObject) json_item.get("after");
        JSONObject before = (JSONObject) json_item.get("before");
        if (json_item.getString("op").equals("CREATE")) {
            int id = after.getInteger("id");
            String name = after.getString("name");
            String phone_number = after.getString("phone_number");
            String pt = json_item.getString("ts_ms");
//                    System.out.println(pt);
            return new user_info(id, name, phone_number, pt,0);
        } else if (json_item.getString("op").equals("DELETE")) {
            int id = before.getInteger("id");
            String name = before.getString("name");
            String phone_number = before.getString("phone_number");
            String pt = "0";
//                    System.out.println(pt);
            return new user_info(id, name, phone_number, pt,1);
        } else {
            int id = after.getInteger("id");
            String name = after.getString("name");
            String phone_number = after.getString("phone_number");
            String pt = json_item.getString("ts_ms");
//                    System.out.println(pt);
            return new user_info(id, name, phone_number, pt,0);
        }

    }

}
