package dx.ict.flatmaps;

import com.alibaba.fastjson.JSONObject;
import dx.ict.pojo.user_info;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author 李二白
 * @date 2023/04/13
 */
public class user_info_flatmapper implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String item, Collector out) throws Exception {
        JSONObject json_item =
                JSONObject.parseObject(item);
//        final JSONObject after = (JSONObject) json_item.get("after");
//        final JSONObject before = (JSONObject) json_item.get("before");
        if (!json_item.getString("op").equals("DELETE")) {
            out.collect(item);
        }
    }
}
