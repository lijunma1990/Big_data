package dx.ict.util;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;


public class KafkaTopicManager {
    // Kafka集群地址
    //private static String bootstrapServers;
    // kafka  topic名称
    //private static String topicName;
    private static String bootstrapServers ; // 更新为实际的Kafka集群地址

    public KafkaTopicManager(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    //public static void main(String[] args) throws Exception {
    //
    //    KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
    //    String topicName = "test_topic";
    //    //if (!manager.topicExists(topicName)) {
    //    //    manager.createTopic(topicName, 3, (short) 2);
    //    //}
    //    //
    //    if (manager.topicExists(topicName)) {
    //        manager.deleteTopic(topicName);
    //    }
    //    //
    //    //manager.listTopics();
    //}

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return properties;
    }

    // 判断topic是否存在
    public boolean topicExists(String topicName) {
        try (AdminClient adminClient = AdminClient.create(getProperties())) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get(); // 同步等待结果
            return topicNames.contains(topicName);
        } catch (Exception e) {
            System.err.println("Error while checking topic existence: " + e);
            return false;
        }
    }

    /**
     * 创建topic
     * topicName--主题;
     * partitions--分区数;
     * replicationFactor--副本数
     */
    public static void createTopic(String topicName, int partitions, short replicationFactor) {
        try (AdminClient adminClient = AdminClient.create(getProperties())) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));

            // 同步以等待主题创建完成
            createTopicsResult.all().get();
            System.out.println("Topic created: " + topicName);
        } catch (Exception e) {
            System.err.println("Error while creating topic: " + e);
        }
    }

    //删除topic
    public static void deleteTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(getProperties())) {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));

            // 同步以等待主题删除完成
            deleteTopicsResult.all().get();
            System.out.println("Topic deleted: " + topicName);
        } catch (Exception e) {
            System.err.println("Error while deleting topic: " + e);
        }
    }

    //获取broker内的topic列表
    public static Set<String> listTopics() {
        try (AdminClient adminClient = AdminClient.create(getProperties())) {
            ListTopicsResult topics = adminClient.listTopics();

            // 访问主题列表
            KafkaFuture<Set<String>> names = topics.names();

            // Synchronize and wait on the future
            Set<String> topicNames =names.get();
            System.out.println("Topics in the cluster: " + topicNames);
            return topicNames;
        } catch (Exception e) {
            System.err.println("Error while listing topics: " + e);
        }
        return null;
    }
}

