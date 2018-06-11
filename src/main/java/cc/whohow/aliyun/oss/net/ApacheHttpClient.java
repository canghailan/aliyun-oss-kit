package cc.whohow.aliyun.oss.net;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.IOException;

public class ApacheHttpClient {
    private static volatile CloseableHttpClient httpClient = HttpClientBuilder.create()
            .setMaxConnPerRoute(512)
            .setMaxConnTotal(1024)
            .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
            .build();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ApacheHttpClient::shutdown));
    }

    public static CloseableHttpClient get() {
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
