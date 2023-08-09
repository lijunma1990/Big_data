package dx.ict.jobs;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.alibaba.fastjson.JSONObject;
import dx.ict.util.GetElasticsearch;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fanwanbing
 * @version 1.0
 * @description: 只能获取10条数据
 * @date 2023/5/24 14:12
 */
public class elasticSearchETL {

    @SneakyThrows
    public static void main(String[] args) {
        ElasticsearchClient clientNew = GetElasticsearch.getClientNew();
        ArrayList<String> strings = new ArrayList<>();
        strings.add("szjc-nglog-tyzc-2023-05-11");
//        strings.add("szjc-nglog-tyzc-2023-05-12");
//        strings.add("szjc-nglog-tyzc-2023-05-13");
//        strings.add("szjc-nglog-tyzc-2023-05-14");
//        strings.add("szjc-nglog-tyzc-2023-05-15");
//        strings.add("szjc-nglog-tyzc-2023-05-16");
//        strings.add("szjc-nglog-tyzc-2023-05-17");
//        strings.add("szjc-nglog-tyzc-2023-05-18");
//        strings.add("szjc-nglog-tyzc-2023-05-19");
//        strings.add("szjc-nglog-tyzc-2023-05-20");

        SearchResponse<JSONObject> search =
                clientNew.search(
                        (SearchRequest.Builder s) -> {
                            return s
                                    .index(strings)
                                    .size(100)
                                    .query((Query.Builder q) -> {
                                        return q.matchAll(m -> m);
                                    });
                        },
                        JSONObject.class
                );
//        System.out.println("search = " + search);

        List<Hit<JSONObject>> hits = search.hits().hits();
        for (Hit<JSONObject> hit : hits) {
            assert hit.source() != null;
            JSONObject source = hit.source();
            String s = source.toString();
            System.out.println("s = " + s);
//            String http_user_agent_ = "http_user_agent=(.*?)\\s";
//            String log = "log=(.*?)\\s";
//            Pattern pattern = Pattern.compile(log);
//            Matcher matcher = pattern.matcher(s);
//            if (matcher.find()) {
//                String http_user_agent = matcher.group(1);
//                System.out.println(http_user_agent);
//            }

        }
    }
}
