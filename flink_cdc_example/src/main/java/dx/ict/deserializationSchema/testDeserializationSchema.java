package dx.ict.deserializationSchema;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
/**
 * @author 李二白
 * @date 2023/04/06
 */
public class testDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private transient JsonConverter jsonConverter;
    private final Boolean includeSchema;
    private Map<String, Object> customConverterConfigs;

    public testDeserializationSchema() {
        this(false);
    }

    public testDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
        System.out.println("includeSchema>>>>>>>>>>>>>>>>>>>>>>>>>>" + this.includeSchema);
    }

    public testDeserializationSchema(Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        System.out.println("includeSchema>>>>>>>>>>>>>>>>>>>>>>>>>>" + this.includeSchema);
        this.customConverterConfigs = customConverterConfigs;
        System.out.println("customConverterConfigs>>>>>>>>>>>>>>>>>>>>>>" + this.customConverterConfigs);
    }

    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        if (this.jsonConverter == null) {
            this.initializeJsonConverter();
        }

//        System.out.println("value>>>>>>>>>>>>>>>>>>>>>>>>>>" + value);
        byte[] bytes = this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
//        System.out.println("record.topic()>>>>>>>>>>>>>>>>>>>>>>>>>>" + record.topic());
//        System.out.println("record.valueSchema()>>>>>>>>>>>>>>>>>>>>>>>>>>" + record.valueSchema());
//        System.out.println(" record.value()>>>>>>>>>>>>>>>>>>>>>>>>>>" + record.value());

        out.collect(new String(bytes));
    }

    private void initializeJsonConverter() {
        this.jsonConverter = new JsonConverter();
        HashMap<String, Object> configs = new HashMap(2);
        configs.put("converter.type", ConverterType.VALUE.getName());
        configs.put("schemas.enable", this.includeSchema);
        if (this.customConverterConfigs != null) {
            configs.putAll(this.customConverterConfigs);
        }

        this.jsonConverter.configure(configs);
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

