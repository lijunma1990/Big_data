package dx.ict.processes;

import com.alibaba.fastjson.JSONObject;
import dx.ict.pojo.user_info;
import dx.ict.pojo.user_info_E;
import dx.ict.restricts.RestrictRule;
import dx.ict.util.QualityCheck;
import dx.ict.enumeration.RestrictionEnum;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Field;

/**
 * @author 李二白
 * @date 2023/04/13
 * 表user_info对应的质量检测配置process
 * 监测列：id->规则：type_check_Int
 * 监测列：name->规则：type_check_String
 * 监测列：phone_number->规则：pattern_check_PhoneNumber
 */
public class user_info_process extends ProcessFunction<String, user_info> {

    @Override
    public void processElement(String item, Context ctx, Collector<user_info> out) throws Exception {
        //使用side output分流
        final OutputTag<user_info> mainStreamTag = new OutputTag<user_info>("main") {
        };
        final OutputTag<user_info_E> errorStreamTag = new OutputTag<user_info_E>("error") {
        };

        //打印pojo类型
        reflect(item);


        int id = 0;
        String name = "";
        String phone_number = "";
        String pt = "0";
        int DELETE_MARK = 0;

        final JSONObject json_item = JSONObject.parseObject(item);
        JSONObject after = (JSONObject) json_item.get("after");
        JSONObject before = (JSONObject) json_item.get("before");
        if (json_item.getString("op").equals("DELETE")) {
            id = before.getInteger("id");
            name = before.getString("name");
            phone_number = before.getString("phone_number");
            pt = json_item.getString("ts_ms");
            DELETE_MARK = 1;

        } else {
            id = after.getInteger("id");
            name = after.getString("name");
            phone_number = after.getString("phone_number");
            pt = json_item.getString("ts_ms");
            DELETE_MARK = 0;
        }

        final RestrictRule idCheck = RestrictionEnum.valueOf("type_check_Int").getRestrictRule();
        final RestrictRule nameCheck = RestrictionEnum.valueOf("type_check_String").getRestrictRule();
        final RestrictRule phone_numberCheck = RestrictionEnum.valueOf("pattern_check_PhoneNumber").getRestrictRule();

        if (!idCheck.ifPass(String.valueOf(id))) {
            final user_info_E ERROR_CLASS = new user_info_E(id, name, phone_number, pt, "id", idCheck.getMsg(), DELETE_MARK);
            ctx.output(errorStreamTag, ERROR_CLASS);
        }
        if (!nameCheck.ifPass(name)) {
            final user_info_E ERROR_CLASS = new user_info_E(id, name, phone_number, pt, "name", nameCheck.getMsg(), DELETE_MARK);
            ctx.output(errorStreamTag, ERROR_CLASS);
        }
        if (!phone_numberCheck.ifPass(phone_number)) {
            final user_info_E ERROR_CLASS = new user_info_E(id, name, phone_number, pt, "phone_number", phone_numberCheck.getMsg(), DELETE_MARK);
            ctx.output(errorStreamTag, ERROR_CLASS);
        }
        if (idCheck.ifPass(String.valueOf(id)) && nameCheck.ifPass(name) && phone_numberCheck.ifPass(phone_number)) {
            final user_info RIGHT_CLASS = new user_info(id, name, phone_number, pt, DELETE_MARK);
            ctx.output(mainStreamTag, RIGHT_CLASS);
        }
        final user_info RIGHT_CLASS = new user_info(id, name, phone_number, pt, DELETE_MARK);
        out.collect(RIGHT_CLASS);

    }


    private Field[] reflect(Object item) throws IllegalAccessException {
        //获得实例对象user_info_item的属性与值
        final Field[] Fields = item.getClass().getDeclaredFields();
        for (Field field : Fields
        ) {
            field.setAccessible(true);
            System.out.println("属性名：" + field.getName() + ",属性值：" + field.get(item) + ",属性类型：" + field.getType());
        }
        return Fields;
    }
}
