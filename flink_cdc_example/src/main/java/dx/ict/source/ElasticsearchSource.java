//package dx.ict.source;
//
//import co.elastic.clients.elasticsearch.core.SearchRequest;
//import co.elastic.clients.elasticsearch.core.SearchResponse;
//import co.elastic.clients.json.jackson.JacksonJsonpMapper;
//import co.elastic.clients.transport.ElasticsearchTransport;
//import co.elastic.clients.transport.rest_client.RestClientTransport;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.http.HttpHost;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//import org.apache.flink.types.Row;
//import org.apache.flink.configuration.Configuration;
//import org.elasticsearch.action.search.SearchAction;
//import org.elasticsearch.action.search.SearchRequestBuilder;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestClientBuilder;
//import co.elastic.clients.elasticsearch.ElasticsearchClient;
//
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
///**
// * @Author 李二白
// * @Description TODO
// * @Date 2023/5/30 14:39
// * @Version 1.0
// */
//
//public class ElasticsearchSource extends RichParallelSourceFunction<Row> {
//    //    private transient RestHighLevelClient client;
//    ElasticsearchClient elasticsearchClient;
//    private final String hostName;
//    private final String query;
//    private final int port;
//    private final String schema;
//    private String user;
//    private String password;
//    private String indexes;
//
//    public ElasticsearchSource(String query, String hostName, int port, String schema, String user, String password, String indexes) {
//        this.query = query;
//        this.hostName = hostName;
//        this.port = port;
//        this.schema = schema;
//        this.user = user;
//        this.password = password;
//        this.indexes = indexes;
//    }
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
////        // 创建 Elasticsearch 客户端
////        List<HttpHost> httpHosts = Arrays.asList(new HttpHost(this.hostName, this.port, this.schema));
////        RestClientBuilder builder = RestClient.builder(String.valueOf(httpHosts.toArray(new HttpHost[0])));
////        client = new RestHighLevelClient(builder);
//        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
//        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName, port));
//        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
//
//                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//            }
//        });
//        RestClient restClient = restClientBuilder.build();
//
//        // Create the transport with a Jackson mapper
//        ElasticsearchTransport transport = new RestClientTransport(
//                restClient, new JacksonJsonpMapper());
//
//        // And create the API client
//        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);
////        return elasticsearchClient;
////        ElasticsearchAsyncClient elasticsearchAsyncClient = new ElasticsearchAsyncClient(transport);
//
//    }
//
//    @Override
//    public void run(SourceContext<Row> ctx) throws Exception {
//        // 构造 index String[] 对象
//        final String[] indexes_array = this.indexes.split(",");
//        List<String> index_list = new ArrayList<>(Arrays.asList(indexes_array));
//
//        // 构造 ES 搜索请求
//        SearchRequest.Builder request = new SearchRequest.Builder()
//                .index(index_list)
//                .query();
//
//
//        final SearchResponse<JSONObject> search = elasticsearchClient.search(request, JSONObject.class);
//        while (true) {
//            // 执行 ES 搜索请求
//            SearchResponse response = elasticsearchClient.search(request);
//            // 将搜索结果发送给 Flink 流处理框架
//            ctx.collect(response);
//            // 等待 5 秒钟后再次执行搜索
//            Thread.sleep(5000);
//        }
//    }
//
//    @Override
//    public void cancel() {
////        // 取消操作，释放资源
////        try {
////            if (client != null) {
////                client.close();
////            }
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//    }
//}