package cc.whohow.aliyun.oss.net;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpURLConnection implements Closeable {
    protected static final CloseableHttpClient httpClient = HttpClientBuilder.create()
            .setMaxConnPerRoute(1024)
            .setMaxConnTotal(1024)
            .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
            .build();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> HttpClientUtils.closeQuietly(httpClient)));
    }

    protected RequestBuilder request;
    protected CloseableHttpResponse response;

    public HttpURLConnection(String url) {
        this(HttpGet.METHOD_NAME, url);
    }

    public HttpURLConnection(URI url) {
        this(HttpGet.METHOD_NAME, url);
    }

    public HttpURLConnection(URL url) {
        this(HttpGet.METHOD_NAME, url);
    }

    public HttpURLConnection(String method, String url) {
        this(method, URI.create(url));
    }

    public HttpURLConnection(String method, URI url) {
        this.request = RequestBuilder.create(method).setUri(url);
    }

    public HttpURLConnection(String method, URL url) {
        this.request = RequestBuilder.create(method).setUri(toURI(url));
    }

    private static URI toURI(URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static URL toURL(URI url) {
        try {
            return url.toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public int getContentLength() {
        return getHeaderFieldInt(HttpHeaders.CONTENT_LENGTH, -1);
    }

    public long getContentLengthLong() {
        return getHeaderFieldLong(HttpHeaders.CONTENT_LENGTH, -1);
    }

    public String getContentType() {
        return getHeaderField(HttpHeaders.CONTENT_TYPE);
    }

    public String getContentEncoding() {
        return getHeaderField(HttpHeaders.CONTENT_ENCODING);
    }

    public long getExpiration() {
        return getHeaderFieldDate(HttpHeaders.EXPIRES, -1);
    }

    public long getDate() {
        return getHeaderFieldDate(HttpHeaders.DATE, -1);
    }

    public long getLastModified() {
        return getHeaderFieldDate(HttpHeaders.LAST_MODIFIED, -1);
    }

    protected CloseableHttpResponse getResponse() {
        try {
            if (response == null) {
                connect();
            }
            return response;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getHeaderFieldKey(int n) {
        Header[] headers = getResponse().getAllHeaders();
        return headers.length < n + 1 ? headers[n].getName() : null;
    }

    public String getHeaderField(int n) {
        Header[] headers = getResponse().getAllHeaders();
        return headers.length < n + 1 ? headers[n].getValue() : null;
    }

    public String getRequestMethod() {
        return request.getMethod();
    }

    public int getResponseCode() throws IOException {
        return getResponse().getStatusLine().getStatusCode();
    }

    public String getResponseMessage() throws IOException {
        return getResponse().getStatusLine().getReasonPhrase();
    }

    public long getHeaderFieldDate(String name, long defaultValue) {
        return getHeaderFieldDate(getResponseHeader(name), defaultValue);
    }

    public InputStream getErrorStream() {
        CloseableHttpResponse response = getResponse();
        if (response.getStatusLine().getStatusCode() >= HttpStatus.SC_BAD_REQUEST) {
            try {
                return response.getEntity().getContent();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }

    public URL getURL() {
        return toURL(request.getUri());
    }

    protected Header getRequestHeader(String name) {
        return request.getFirstHeader(name);
    }

    protected Header getResponseHeader(String name) {
        return getResponse().getFirstHeader(name);
    }

    protected String getHeaderField(Header header, String defaultValue) {
        if (header == null || header.getValue() == null) {
            return defaultValue;
        }
        return header.getValue();
    }

    protected long getHeaderFieldDate(Header header, long defaultValue) {
        if (header == null || header.getValue() == null || header.getValue().isEmpty()) {
            return defaultValue;
        }
        Date date = DateUtils.parseDate(header.getValue());
        return date == null ? defaultValue : date.getTime();
    }

    protected long getHeaderFieldLong(Header header, long defaultValue) {
        if (header == null || header.getValue() == null || header.getValue().isEmpty()) {
            return defaultValue;
        }
        return Long.parseLong(header.getValue());
    }

    protected Integer getHeaderFieldInt(Header header, int defaultValue) {
        if (header == null || header.getValue() == null || header.getValue().isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(header.getValue());
    }

    public String getHeaderField(String name) {
        return getHeaderField(getResponseHeader(name), null);
    }

    public Map<String, List<String>> getHeaderFields() {
        return Arrays.stream(getResponse().getAllHeaders())
                .collect(Collectors.groupingBy(
                        Header::getName,
                        Collectors.mapping(Header::getValue, Collectors.toList())));
    }

    public int getHeaderFieldInt(String name, int defaultValue) {
        return getHeaderFieldInt(getResponseHeader(name), defaultValue);
    }

    public long getHeaderFieldLong(String name, long defaultValue) {
        return getHeaderFieldLong(getResponseHeader(name), defaultValue);
    }

    public InputStream getInputStream() throws IOException {
        return getResponse().getEntity().getContent();
    }

    public long getIfModifiedSince() {
        return getHeaderFieldDate(getRequestHeader(HttpHeaders.IF_MODIFIED_SINCE), -1);
    }

    public void setIfModifiedSince(long ifModifiedSince) {
        request.setHeader(HttpHeaders.IF_MODIFIED_SINCE, DateUtils.formatDate(new Date(ifModifiedSince)));
    }

    public void setRequestProperty(String key, String value) {
        request.setHeader(key, value);
    }

    public void addRequestProperty(String key, String value) {
        request.addHeader(key, value);
    }

    public String getRequestProperty(String key) {
        return getHeaderField(getRequestHeader(key), null);
    }

    public void disconnect() {
        HttpClientUtils.closeQuietly(response);
        response = null;
    }

    public synchronized void connect() throws IOException {
        if (response == null) {
            response = httpClient.execute(request.build());
        }
    }

    @Override
    public void close() throws IOException {
        disconnect();
    }
}
