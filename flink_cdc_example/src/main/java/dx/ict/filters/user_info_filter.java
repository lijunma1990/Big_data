package dx.ict.filters;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author 李二白
 * @date 2023/04/06
 */
public class user_info_filter implements FilterFunction<String> {

    @Override
    public boolean filter(String item) throws Exception {
        return figureDeleteMsg(item);
    }

    private boolean figureDeleteMsg(String item) {
        JSONObject json_item = JSONObject.parseObject(item);
        //若该记录类型为delete，则将其过滤掉
        return !json_item.getString("op").equals("DELETE");
    }
}
