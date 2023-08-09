package dx.ict.jobs;

import com.alibaba.fastjson.JSONObject;
import dx.ict.util.GetElasticsearch;
//import dx.ict.util.ExecHive;
import dx.ict.others.ExecMysql;
import lombok.SneakyThrows;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author fanwanbing
 * @version 1.0
 * @description: TODO
 * @date 2023/5/23 10:24
 */
public class esTohive {


    @SneakyThrows
    public static void main(String[] args) {
        ArrayList<String> strings = new ArrayList<>();
        for (int i = 11; i < 21; i++) {
            strings.add("szjc-nglog-tyzc-2023-05-".concat(String.valueOf(i)));
        }

        for (String sourceIndex : strings) {
            System.out.println(sourceIndex);
            selectEsToMysqlES(sourceIndex);
        }
    }

//    @SneakyThrows
//    public static void selectEsToHiveES(String sourceIndex) {
//        /*hive直接导入，批量导入不可行，转为flink导入*/
//        SearchRequest SearchRequestRequest;
//        RestHighLevelClient client = GetElasticsearch.getClient();
//        String sql;
//        PreparedStatement pstmt = null;
//        try {
//            SearchRequest searchRequest = new SearchRequest(sourceIndex);
//            //请求设置
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//            searchSourceBuilder.fetchSource(true);
//            searchSourceBuilder.size(1000);
//            searchRequest.scroll(TimeValue.timeValueMinutes(5L));
//            searchRequest.source(searchSourceBuilder);
////            请求发送
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            SearchHit[] searchHit = searchResponse.getHits().getHits();
//            for (SearchHit hit : searchHit) {
//                String sourceAsString = hit.getSourceAsString(); // 获取document
//                JSONObject object = JSONObject.parseObject(sourceAsString);
//                /*String body_bytes_sent =object.getString("body_bytes_sent");
//                String request_id =object.getString("request_id");
//                String request =object.getString("request");
//                String time_local =object.getString("time_local");
//                String http_referer =object.getString("http_referer");
//                String upstream_addr =object.getString("upstream_addr");
//                String upstream_status =object.getString("upstream_status");
//                String gzip_ratio =object.getString("gzip_ratio");
//                String log =object.getString("log");
//                String timestamp_ =object.getString("timestamp_");
//                String http_host =object.getString("http_host");
//                String status =object.getString("status");
//                String ssl_cipher =object.getString("ssl_cipher");
//                String url =object.getString("url");
//                String http_user_agent =object.getString("http_user_agent");
//                String connection_requests =object.getString("connection_requests");
//                String ssl_protocol =object.getString("ssl_protocol");
//                String http_version =object.getString("http_version");
//                String platform =object.getString("platform");
//                String connection =object.getString("connection");
//                String upstream_response_time =object.getString("upstream_response_time");
//                String http_x_forwarded_for =object.getString("http_x_forwarded_for");
//                String remote_addr =object.getString("remote_addr");
//                String remote_user =object.getString("remote_user");
//                String request_time =object.getString("request_time");
//                String http_port  =object.getString("http_port");*/
//
//                sql = "insert into temp.es_table (body_bytes_sent," +
//                        "request_id,request,time_local,http_referer,upstream_addr," +
//                        "upstream_status,gzip_ratio,log,timestamp_,http_host,status," +
//                        "ssl_cipher,url,http_user_agent,connection_requests,ssl_protocol," +
//                        "http_version,platform,connection,upstream_response_time," +
//                        "http_x_forwarded_for,remote_addr,remote_user," +
//                        "request_time,http_port)\n" +
//                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
//                pstmt = ExecHive.insertSQL(sql);
//                pstmt.setString(1, object.getString("body_bytes_sent"));
//                pstmt.setString(2, object.getString("request_id"));
//                pstmt.setString(3, object.getString("request"));
//                pstmt.setString(4, object.getString("time_local"));
////                System.out.println("object.getString(\"time_local\") = " + object.getString("time_local"));
//                pstmt.setString(5, object.getString("http_referer"));
//                pstmt.setString(6, object.getString("upstream_addr"));
//                pstmt.setString(7, object.getString("upstream_status"));
//                pstmt.setString(8, object.getString("gzip_ratio"));
//                pstmt.setString(9, object.getString("log"));
//                pstmt.setString(10, object.getString("@timestamp"));
//                pstmt.setString(11, object.getString("http_host"));
//                pstmt.setString(12, object.getString("status"));
//                pstmt.setString(13, object.getString("ssl_cipher"));
//                pstmt.setString(14, object.getString("url"));
//                pstmt.setString(15, object.getString("http_user_agent"));
//                pstmt.setString(16, object.getString("connection_requests"));
//                pstmt.setString(17, object.getString("ssl_protocol"));
//                pstmt.setString(18, object.getString("http_version"));
//                pstmt.setString(19, object.getString("platform"));
//                pstmt.setString(20, object.getString("connection"));
//                pstmt.setString(21, object.getString("upstream_response_time"));
//                pstmt.setString(22, object.getString("http_x_forwarded_for"));
//                pstmt.setString(23, object.getString("remote_addr"));
//                pstmt.setString(24, object.getString("remote_user"));
//                pstmt.setString(25, object.getString("request_time"));
//                pstmt.setString(26, object.getString("http_port"));
//                pstmt.execute();
//
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        if (pstmt != null) {
//            pstmt.close();
//        }
////        pstmt.executeBatch();
//    }


    public static void selectEsToHiveFlink(String sourceIndex) {

    }

    @SneakyThrows
    public static void selectEsToMysqlES(String sourceIndex) {
        /*es数据导入mysql*/
        SearchRequest SearchRequestRequest;
        RestHighLevelClient client = GetElasticsearch.getClient();
        String sql;
        PreparedStatement pstmt = null;
        Connection conn = ExecMysql.getConnection();
        try {
            SearchRequest searchRequest = new SearchRequest(sourceIndex);
            //请求设置
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.fetchSource(true);
            searchSourceBuilder.size(1000);// 每次滚动查询返回的文档数
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));
//            searchSourceBuilder.sort("sort_field", SortOrder.ASC); // 滚动查询结果的排序方式
            searchRequest.source(searchSourceBuilder);
            // 设置滚动查询的时间间隔
            TimeValue scrollTime = TimeValue.timeValueMinutes(1);
//            请求发送
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHit = searchResponse.getHits().getHits();
            while (searchHit != null && searchHit.length > 0) {
                for (SearchHit hit : searchHit) {
                    String sourceAsString = hit.getSourceAsString(); // 获取document
                    JSONObject object = JSONObject.parseObject(sourceAsString);
                    System.out.println("object = " + object);
                /*String body_bytes_sent =object.getString("body_bytes_sent");
                String request_id =object.getString("request_id");
                String request =object.getString("request");
                String time_local =object.getString("time_local");
                String http_referer =object.getString("http_referer");
                String upstream_addr =object.getString("upstream_addr");
                String upstream_status =object.getString("upstream_status");
                String gzip_ratio =object.getString("gzip_ratio");
                String log =object.getString("log");
                String timestamp_ =object.getString("timestamp_");
                String http_host =object.getString("http_host");
                String status =object.getString("status");
                String ssl_cipher =object.getString("ssl_cipher");
                String url =object.getString("url");
                String http_user_agent =object.getString("http_user_agent");
                String connection_requests =object.getString("connection_requests");
                String ssl_protocol =object.getString("ssl_protocol");
                String http_version =object.getString("http_version");
                String platform =object.getString("platform");
                String connection =object.getString("connection");
                String upstream_response_time =object.getString("upstream_response_time");
                String http_x_forwarded_for =object.getString("http_x_forwarded_for");
                String remote_addr =object.getString("remote_addr");
                String remote_user =object.getString("remote_user");
                String request_time =object.getString("request_time");
                String http_port  =object.getString("http_port");*/
                    sql = "insert into fwb_test.es_table (body_bytes_sent," +
                            "request_id,request,time_local,http_referer,upstream_addr," +
                            "upstream_status,gzip_ratio,log,timestamp_,http_host,status," +
                            "ssl_cipher,url,http_user_agent,connection_requests,ssl_protocol," +
                            "http_version,platform,connection,upstream_response_time," +
                            "http_x_forwarded_for,remote_addr,remote_user," +
                            "request_time,http_port,index_)\n" +
                            "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
                    System.out.println("sql = " + sql);
                    pstmt = conn.prepareStatement(sql);
                    pstmt.setString(1, object.getString("body_bytes_sent"));
                    pstmt.setString(2, object.getString("request_id"));
                    pstmt.setString(3, object.getString("request"));
                    pstmt.setString(4, object.getString("time_local"));
                    pstmt.setString(5, object.getString("http_referer"));
                    pstmt.setString(6, object.getString("upstream_addr"));
                    pstmt.setString(7, object.getString("upstream_status"));
                    pstmt.setString(8, object.getString("gzip_ratio"));
                    pstmt.setString(9, object.getString("log"));
                    pstmt.setString(10, object.getString("@timestamp"));
                    pstmt.setString(11, object.getString("http_host"));
                    pstmt.setString(12, object.getString("status"));
                    pstmt.setString(13, object.getString("ssl_cipher"));
                    pstmt.setString(14, object.getString("url"));
                    pstmt.setString(15, object.getString("http_user_agent"));
                    pstmt.setString(16, object.getString("connection_requests"));
                    pstmt.setString(17, object.getString("ssl_protocol"));
                    pstmt.setString(18, object.getString("http_version"));
                    pstmt.setString(19, object.getString("platform"));
                    pstmt.setString(20, object.getString("connection"));
                    pstmt.setString(21, object.getString("upstream_response_time"));
                    pstmt.setString(22, object.getString("http_x_forwarded_for"));
                    pstmt.setString(23, object.getString("remote_addr"));
                    pstmt.setString(24, object.getString("remote_user"));
                    pstmt.setString(25, object.getString("request_time"));
                    pstmt.setString(26, object.getString("http_port"));
                    pstmt.setString(27, sourceIndex);
//                pstmt.addBatch();
//                if (i % 1000 == 0){
//                    System.out.println("i = "+ i);
//                    i = 0;
//                    pstmt.executeBatch();
//                    pstmt.clearBatch();
//                }
//                i++;
                    pstmt.execute();
                }
                //发起下一次滚动查询
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scrollTime);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHit = searchResponse.getHits().getHits();
            }

            //清楚滚动查询上下文
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
//            assert pstmt != null;
//            pstmt.executeBatch();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("pstmt = " + pstmt);
            assert pstmt != null;
            pstmt.close();
            conn.close();
            client.close();
        }

    }
}
