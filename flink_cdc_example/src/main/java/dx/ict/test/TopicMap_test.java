package dx.ict.test;

import dx.ict.util.TopicMap;

import java.util.Iterator;
import java.util.List;

public class TopicMap_test {
    public static void main(String[] args) {

        final TopicMap topicMap = new TopicMap("172.32.0.67:9092，172.32.0.68:9092，172.32.0.69:9092，172.32.0.70:9092，172.32.0.71:9092");
        final List<String> topic_list = topicMap.List_All_Topic();
        topicMap.clear_all();
    }
}
