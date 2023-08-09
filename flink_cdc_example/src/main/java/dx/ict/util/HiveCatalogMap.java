package dx.ict.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author 李二白
 * @date 2023/04/06
 */

public class HiveCatalogMap implements Serializable {
    final private Map<String, Properties> HiveCatalogMap = new HashMap<>();

    public HiveCatalogMap() {
        /*
         * initializer
         * 初始化构造HiveCatalogMap
         * */
        final Properties hive_properties = new Properties();
        /*
         * 尝试从setting文件src\\main\\resources\\setting.properties中获得对应键值
         try {
         properties.load(new FileReader("src\\main\\resources\\setting.properties"));
         } catch (IOException e) {
         e.printStackTrace();
         }*/
        hive_properties.put("catalogName", "myhive");
        hive_properties.put("defaultDatabase", "kafka");
        hive_properties.put("hiveConfDir", "/etc/hive/conf.cloudera.hive");
        hive_properties.put("hadoopConfDir", "");
        hive_properties.put("hiveVersion", "2.3.9");
        this.HiveCatalogMap.put("default", hive_properties);
    }

    public void add_Catalog(String catalogName,
                            String defaultDatabase,
                            String hiveConfDir,
                            String hadoopConfDir,
                            String hiveVersion
    ) {
        /*
         * 用此方法添加hive的catalog配置
         * */
        final Properties hive_properties = new Properties();
        /*
         * 尝试从setting文件src\\main\\resources\\setting.properties中获得对应键值
         try {
         properties.load(new FileReader("src\\main\\resources\\setting.properties"));
         } catch (IOException e) {
         e.printStackTrace();
         }*/
        hive_properties.put("catalogName", catalogName);
        hive_properties.put("defaultDatabase", defaultDatabase);
        hive_properties.put("hiveConfDir", hiveConfDir);
        hive_properties.put("hadoopConfDir", hadoopConfDir);
        hive_properties.put("hiveVersion", hiveVersion);
        this.HiveCatalogMap.put(catalogName, hive_properties);
    }

    public void remove_Catalog(String catalogName) {
        /*
         * 用此方法删除hive的catalog配置
         * */
        this.HiveCatalogMap.remove(catalogName);
    }

    public StreamTableEnvironment create_tableEnv(StreamExecutionEnvironment env, String catalogName) {

        StreamTableEnvironment hiveTableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());
        hiveTableEnv.getConfig().addConfiguration(new Configuration()
                .set(CoreOptions.DEFAULT_PARALLELISM, 1)
                .set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(800))
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30)));
        hiveTableEnv.registerCatalog(catalogName, this.use_Catalog(catalogName));
//        hiveTableEnv.executeSql("USE CATALOG hive_catalog");
        return hiveTableEnv;
    }

    private HiveCatalog use_Catalog(String catalogName) {
        return new HiveCatalog(
                this.HiveCatalogMap.get(catalogName).getProperty("catalogName"),
                this.HiveCatalogMap.get(catalogName).getProperty("defaultDatabase"),
                this.HiveCatalogMap.get(catalogName).getProperty("hiveConfDir"),
                this.HiveCatalogMap.get(catalogName).getProperty("hadoopConfDir"),
                this.HiveCatalogMap.get(catalogName).getProperty("hiveVersion")
        );

    }

    public Map<String, List<String>> tree_catalog(String CatalogName) {
        /*
         * 通过所选的CatalogName进行查询，列出Catalog中包含的库与表
         * 其中，表放在List<String>中
         * */
        final Map<String, List<String>> catalog_tree = new HashMap<>();

        this.view_catalog(catalog_tree);
        return catalog_tree;
    }

    private void view_catalog(Map<String, List<String>> catalog_tree) {
        /*
         * 通过所选的CatalogName进行查询，列出Catalog中包含的库与表
         * 格式如下：
         *           +----------------------+
         *           |hive_catalog_name     |
         *           +----------------------+
         *           |database01            |
         *           +----------------------+
         *           |               table01|
         *           |               table02|
         *           +----------------------+
         *           |database02            |
         *           +----------------------+
         *           |               table03|
         *           |                   ...|
         *           +----------------------+
         *
         * */
    }

}

