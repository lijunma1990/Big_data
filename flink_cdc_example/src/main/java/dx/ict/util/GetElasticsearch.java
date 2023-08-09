package dx.ict.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dx.ict.util.ConfigLoader;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/30 13:33
 * @Version 1.0
 */
public class GetElasticsearch {
    public static String host = ConfigLoader.get("elasticsearch.host");
    public static int port = ConfigLoader.getInt("elasticsearch.port");
    public static String http = ConfigLoader.get("elasticsearch.http");
    public static String user = ConfigLoader.get("elasticsearch.user");
    public static String password = ConfigLoader.get("elasticsearch.password");

    public static RestHighLevelClient getClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, http)
                )
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(AuthScope.ANY,
                                        new UsernamePasswordCredentials(user, password));
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        })
        );
    }

    public static ElasticsearchClient getClientNew() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, port));
        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {

                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestClient restClient = restClientBuilder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);
        return elasticsearchClient;
//        ElasticsearchAsyncClient elasticsearchAsyncClient = new ElasticsearchAsyncClient(transport);
    }
}
