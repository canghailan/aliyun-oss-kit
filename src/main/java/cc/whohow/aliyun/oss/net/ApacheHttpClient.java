package cc.whohow.aliyun.oss.net;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;

public class ApacheHttpClient {
    private static volatile CloseableHttpClient httpClient = HttpClientBuilder.create()
            .setConnectionManager(new PoolingHttpClientConnectionManager())
            .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
            .build();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ApacheHttpClient::shutdown));
    }

    public static HttpClient get() {
        return httpClient;
    }

    private static void shutdown() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (IOException ignore) {
        }
    }
}
