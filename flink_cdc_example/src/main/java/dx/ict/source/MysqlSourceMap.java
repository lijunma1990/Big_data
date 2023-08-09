package dx.ict.source;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import dx.ict.deserializationSchema.MySqlDeserialization;
import dx.ict.enumeration.MySqlDeserializationSchemaEnum;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/3/26 16:53
 * @Version 1.0
 */

public class MysqlSourceMap implements Serializable {
    final private Map<String, Properties> MysqlSources = new HashMap<>();

    public MysqlSourceMap() {
        /*
         * initializer
         * 存放<source名,source配置>对应的配置，
         * 真正的source只有在调用本实例的get方法的时候才会生成source的生成器,
         * 生成器在build()后方可使用。
         * */
        final Properties defaultProperty = new Properties();
        defaultProperty.put("hostname", "localhost");
        defaultProperty.put("port", "3306");
        defaultProperty.put("databaseList", "ljm_test");
        defaultProperty.put("tableList", "ljm_test.user_info");
        defaultProperty.put("username", "root");
        defaultProperty.put("password", "mysql");
        defaultProperty.put("serverId", "5400");
        defaultProperty.put("default", "JsonDebeziumDeserializationSchema");
        this.MysqlSources.put("default", defaultProperty);
    }


    public void add_source(String SourceName_header,
                           String hostname,
                           String port,
                           String databaseList,
                           String tableList,
                           String username,
                           String password,
                           String serverId,
                           String DeserializationSchema
    ) {
        String current_SourceName;
//        String[] table_list = tableList.split(",");
//        for (String item : table_list) {
        current_SourceName = SourceName_header.concat("->").concat(tableList);
        Properties localProperty = new Properties();
        localProperty.put("hostname", hostname);
        localProperty.put("port", port);
        localProperty.put("databaseList", databaseList);
        //默认为ljm_test
        localProperty.put("tableList", tableList);
        localProperty.put("username", username);
        localProperty.put("password", password);
        localProperty.put("serverId", serverId);
        localProperty.put("DeserializationSchema", DeserializationSchema);
        //默认为JsonDebeziumDeserializationSchema，如果为自定义，则选项为"MyDeserializationSchema"
        this.MysqlSources.put(current_SourceName, localProperty);
//        }

    }

    public void remove_source(String SourceName) {
        /*
         * 用此方法删除mysql的source配置
         * */
        this.MysqlSources.remove(SourceName);
    }


    public void remove_sources(String regex) {
        /*
         * 用此方法删除MysqlSources符合正则的所有sink配置
         * */

        //编译生成正则pattern,并进行删除
        Pattern pattern = Pattern.compile(regex);
        for (Iterator<String> item = this.MysqlSources.keySet().iterator(); item.hasNext(); ) {
            String item_ = item.next();
            Matcher matcher = pattern.matcher(item_);
            if (matcher.matches()) {
                item.remove();
                System.out.println("delete: " + item);
            } else {
                System.out.println("keep: " + item);
            }
        }
    }

    private MySqlSourceBuilder get_builder(String SourceName) {
        String hostname = this.MysqlSources.get(SourceName).getProperty("hostname");
        String databaseList = this.MysqlSources.get(SourceName).getProperty("databaseList");
        int port = Integer.parseInt(this.MysqlSources.get(SourceName).getProperty("port"));
        String username = this.MysqlSources.get(SourceName).getProperty("username");
        String password = this.MysqlSources.get(SourceName).getProperty("password");
        String tableList = this.MysqlSources.get(SourceName).getProperty("tableList");
        String serverId = this.MysqlSources.get(SourceName).getProperty("serverId");

        MySqlSourceBuilder<String> MySqlSourceBuilder = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password);

        if (databaseList != null && !databaseList.equals("")) {
            MySqlSourceBuilder.databaseList(databaseList);
        }
        if (tableList != null && !tableList.equals("")) {
            MySqlSourceBuilder.tableList(tableList);
        }
        if (serverId != null && !serverId.equals("")) {
            MySqlSourceBuilder.serverId(serverId);
        }
        return MySqlSourceBuilder;
    }


    public MySqlSourceBuilder get_source(String SourceName) {
        /*
         * @default:
         * if we dont put any property into MySourceMap ,
         * then we can get default source with key word "default"
         * @example:
         * MySqlSource
         *     .<String>builder()
         *     .hostname("localhost")
         *     .port(3306)
         *     .databaseList("mydb")
         *     .tableList("mydb.users")
         *     .username(username)
         *     .password(password)
         *     .serverId(5400)
         *     .deserializer(new JsonDebeziumDeserializationSchema())
         *     .build();
         *  */

        final String DeserializationSchema = this.MysqlSources.get(SourceName).getProperty("DeserializationSchema");

        final MySqlSourceBuilder<String> MySqlSourceBuilder = get_builder(SourceName);

        String[] DeserializationSchemaList = MySqlDeserializationSchemaEnum.getEnumTypes();
        for (String item : DeserializationSchemaList
        ) {
            if (item.equals(DeserializationSchema)) {
                final MySqlDeserialization deserializationSchema =
                        MySqlDeserializationSchemaEnum.valueOf(item).getDeserializationSchema();
                MySqlSourceBuilder.deserializer(
                        (DebeziumDeserializationSchema) deserializationSchema
                );
                return MySqlSourceBuilder;
            }
        }
        MySqlSourceBuilder.deserializer(new JsonDebeziumDeserializationSchema());
        return MySqlSourceBuilder;
    }


    public MySqlSourceBuilder<Tuple2<String, Row>> get_source(String SourceName, Map rowTypeMap) {
        /*
         * @default:
         * if we dont put any property into MySourceMap ,
         * then we can get default source with key word "default"
         * @example:
         * MySqlSource
         *     .<String>builder()
         *     .hostname("localhost")
         *     .port(3306)
         *     .databaseList("mydb")
         *     .tableList("mydb.users")
         *     .username(username)
         *     .password(password)
         *     .serverId(5400)
         *     .deserializer(new JsonDebeziumDeserializationSchema())
         *     .build();
         *  */
        final String DeserializationSchema = this.MysqlSources.get(SourceName).getProperty("DeserializationSchema");

        final MySqlSourceBuilder MySqlSourceBuilder = get_builder(SourceName);

        String[] DeserializationSchemaList = MySqlDeserializationSchemaEnum.getEnumTypes();
        for (String item : DeserializationSchemaList
        ) {
            final MySqlDeserialization deserializationSchema =
                    MySqlDeserializationSchemaEnum.valueOf(item).getDeserializationSchema();
            deserializationSchema.setTableRowTypeMap(rowTypeMap);
            deserializationSchema.init();
            if (item.equals(DeserializationSchema)) {
                MySqlSourceBuilder.deserializer(
                        (DebeziumDeserializationSchema) deserializationSchema
                );
                return MySqlSourceBuilder;
            }
        }
        MySqlSourceBuilder.deserializer(new JsonDebeziumDeserializationSchema());
        return MySqlSourceBuilder;
    }
}