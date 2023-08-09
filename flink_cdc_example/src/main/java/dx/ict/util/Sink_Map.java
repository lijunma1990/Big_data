package dx.ict.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author 李二白
 * @date 2023/03/31
 * Map类模板
 * 作用：生成各类map的接口
 */
public interface Sink_Map {

    Map<String, Properties> XXX_Map = new HashMap<String, Properties>();

    void add_sink();
//        final Properties localProperty = new Properties();
//        ...


    void remove_sink(String XXX_name);
//        ...
//        this.XXX_Map.remove(XXX_name);


    Object get_sink(String XXX_name);
//        return null;

}
