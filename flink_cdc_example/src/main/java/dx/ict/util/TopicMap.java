package dx.ict.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 李二白
 * @author xujunfeng
 * @date 2023/03/30
 * kafka主题管理
 */

public class TopicMap {
    private String bootstrapServers;
    private final List<String> Topic_List = new ArrayList<>();
    private final List<String> bootstrapServers_List = new ArrayList<>();
    private String regex;

    public TopicMap(String bootstrapServers) {
        /*
         * 初始化该topicMap，填充topic列表
         * @brokers :kafka的brokers列表，以逗号分割-->"host01,host02,host03..."
         * */
        this.init(bootstrapServers);
    }


    public TopicMap(String bootstrapServers, String regex) {
        /*
         * 初始化该topicMap，填充topic列表
         * @pattern :正则匹配表达式，只要符合该正则匹配的topics均会被添加到list当中
         * @brokers :kafka的brokers列表，以逗号分割-->"host01,host02,host03..."
         * */

        this.init(bootstrapServers, regex);
    }

    public TopicMap() {
        /*
         * 初始化该topicMap，填充topic列表
         * @brokers :kafka的brokers列表，以逗号分割-->"host01,host02,host03..."
         * */
        this.init();
    }

    private void init() {
        /*
         * 初始化该topicMap，填充topic列表
         * */
        this.init("", "");
    }

    private void init(String bootstrapServers) {
        /*
         * 初始化该topicMap，填充topic列表
         * */
        this.init(bootstrapServers, "^.*");
    }

    private void init(String bootstrapServers, String regex) {
        /*
         * 初始化该topicMap，填充topic列表
         * */
        this.bootstrapServers = bootstrapServers;
        this.regex = regex;
        String[] tmp_str = this.bootstrapServers.split(",");
        this.bootstrapServers_List.addAll(Arrays.asList(tmp_str));
        for (String broker_item : this.bootstrapServers_List
        ) {
            List<String> list_tmp = this.List_Topic(broker_item);
            for (String Topic_item : list_tmp
            ) {
                if (!this.isExist(Topic_item)) {
                    this.Topic_List.add(Topic_item);
                }
            }
        }
        //编译生成正则pattern,并删除Topic_List中不符合正则的topic
        Pattern pattern = Pattern.compile(this.regex);
//        for (String Topic_item_ : this.Topic_List
//        ) {
//               Matcher matcher = pattern.matcher(Topic_item_);
//            if (!matcher.matches()) {
//                this.Topic_List.remove(Topic_item_);
//            }
//        }//此处会抛出java.util.ConcurrentModificationException 异常，
//        //改成Iterator的迭代方式，用内部类Itr的remove方法来删除，保证一致性：
        for (Iterator<String> item = this.Topic_List.iterator(); item.hasNext(); ) {
            String item_ = item.next();
            Matcher matcher = pattern.matcher(item_);
            if (!matcher.matches()) {
                item.remove();
                System.out.println("Filter Out: " + item_);

            } else {
                System.out.println("Keep Topic: " + item_);
            }
        }
    }

    public boolean isExist(String item) {
        /*
         * 显示所提及的item是否存在于Topic_List中
         * */
        return !this.notExist(item);

    }

    public boolean notExist(String item) {
        /*
         * 显示所提及的item是否不存在于Topic_List中
         * */
        return !this.Topic_List.contains(item);

    }

    public void addTopic(String topic_name) {
        /*
         * 增加topic  默认 topic 的副本数为2,分区数为3
         * */
        KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
        if (!manager.topicExists(topic_name) && topic_name != null) {
            KafkaTopicManager.createTopic(topic_name, 3, (short) 2);
        }
    }

    public void addTopic(String topic_name,int partitions) {
        /*
         * 增加topic  默认 topic 的副本数为2,分区数为3
         * */
        KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
        if (!manager.topicExists(topic_name) && topic_name != null) {
            KafkaTopicManager.createTopic(topic_name, partitions, (short) 2);
        }
    }

    // 删除列表里的某个topic
    public void removeTopic(String topic_name) {

        KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
        if (manager.topicExists(topic_name)) {
            KafkaTopicManager.deleteTopic(topic_name);
        }
        this.refresh();
    }


    public void clear_all() {
        //清空topicMap以及其对应的topic
        for (Iterator<String> iter = this.Topic_List.iterator(); iter.hasNext();
        ) {
            String item_ = iter.next();
            iter.remove();
            this.removeTopic(item_);
        }

    }

    public static List<String> List_Topic(String bootstrapServers) {
        /*
         * 返回broker中的所有topic
         * */

        KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
        Set<String> topics = KafkaTopicManager.listTopics();
        // 创建一个List集合用于存放符合条件的数据
        assert topics != null;
        return new ArrayList<>(topics);
    }


    public List<String> List_All_Topic() {
        // 返回TopicMap的Topic_List列表中的所有topic
        return this.Topic_List;
    }

    public List<String> List_All_Topic(String regex) {
        /*
         * 返回TopicMap的Topic_List列表中的所有
         * 符合正则匹配的topic*/
//        KafkaTopicManager manager = new KafkaTopicManager(bootstrapServers);
//        Set<String> topics = manager.listTopics();
//        System.out.println(topics);
//
//        // 定义一个正则表达式
//        String regex1 = regex;
//
//        Pattern pattern = Pattern.compile(regex1);
//        // 创建一个List集合用于存放符合条件的数据
//        List<String> matchedList = new ArrayList<>();
//        // 遍历Set集合，使用正则表达式匹配符合条件的数据并添加到List集合中
//        for (String str : topics) {
//            Matcher matcher = pattern.matcher(str);
//            if (matcher.matches()) {
//                matchedList.add(str);
//            }
//        }
        //编译生成正则pattern
        List<String> matchedList = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex);
        for (String Topic_item_ : this.Topic_List
        ) {
            Matcher matcher = pattern.matcher(Topic_item_);
            if (matcher.matches()) {
                matchedList.add(Topic_item_);
            }
        }
        return matchedList;
    }


    public void refresh() {
        /*
         * 刷新topicMap，
         * 更新其中的Topic_List
         * */
        this.Topic_List.clear();
        if (this.regex.equals("")) {
            this.init(this.bootstrapServers);
        } else {
            this.init(this.bootstrapServers, this.regex);
        }
    }

}