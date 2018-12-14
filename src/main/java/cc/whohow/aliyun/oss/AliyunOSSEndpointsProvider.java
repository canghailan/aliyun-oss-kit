package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.net.Ping;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.regions.Endpoint;
import com.aliyuncs.regions.InternalEndpointsParser;
import com.aliyuncs.regions.ProductDomain;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <a href="https://www.alibabacloud.com/help/zh/doc-detail/31837.htm">OSS开通Region和Endpoint对照表</a>
 */
public class AliyunOSSEndpointsProvider {
    protected static final String DEFAULT_ENDPOINT = "oss.aliyuncs.com";
    protected static final String DEFAULT_INTERNAL_ENDPOINT = "oss-internal.aliyuncs.com";
    protected static final Pattern ENDPOINT = Pattern.compile("^(?<sub>.+)\\.aliyuncs\\.com$");
    protected static final List<String> ENDPOINTS = loadEndpoints();
    protected static volatile CompletableFuture<Long> PING = CompletableFuture.supplyAsync(() -> Ping.ping(DEFAULT_INTERNAL_ENDPOINT));

    private static List<String> loadEndpoints() {
        try {
            return new InternalEndpointsParser().getEndpoints().stream()
                    .map(Endpoint::getProductDomains)
                    .flatMap(List::stream)
                    .filter(self -> "oss".equalsIgnoreCase(self.getProductName()))
                    .map(ProductDomain::getDomianName)
                    .distinct()
                    .collect(Collectors.toList());
        } catch (ClientException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    public static String getDefaultEndpoint() {
        return DEFAULT_ENDPOINT;
    }

    public static List<String> getEndpoints() {
        return ENDPOINTS;
    }

    public static String getEndpoint(String endpoint) {
        return PING.join() > 0 ? getIntranetEndpoint(endpoint) : getExtranetEndpoint(endpoint);
    }

    public static String getExtranetEndpoint(String endpoint) {
        String sub = getSub(endpoint);
        if (sub.endsWith("-internal")) {
            return sub.substring(0, sub.length() - "-internal".length()) + ".aliyuncs.com";
        } else {
            return endpoint;
        }
    }

    public static String getIntranetEndpoint(String endpoint) {
        String sub = getSub(endpoint);
        if (sub.endsWith("-internal")) {
            return endpoint;
        } else {
            return sub + "-internal.aliyuncs.com";
        }
    }

    private static String getSub(String endpoint) {
        Matcher matcher = ENDPOINT.matcher(endpoint);
        if (matcher.matches()) {
            return matcher.group("sub");
        }
        throw new IllegalArgumentException(endpoint);
    }
}
