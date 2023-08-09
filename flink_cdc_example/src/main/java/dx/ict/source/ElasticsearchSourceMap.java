package dx.ict.source;

import dx.ict.util.Source_Map;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/30 14:27
 * @Version 1.0
 */
public class ElasticsearchSourceMap implements Serializable, Source_Map {
    final private Map<String, Properties> ElasticsearchSources = new HashMap<>();


    @Override
    public void add_source() {

    }

    @Override
    public void remove_source(String XXX_name) {

    }

    @Override
    public Object get_source(String XXX_name) {
        return null;
    }


}
