package cc.whohow.aliyun.oss;

import java.net.URI;
import java.util.Objects;

/**
 * OSS URI
 * <pre>
 * oss://[accessKeyId]:[secretAccessKey]@[bucketName].[endpoint]/[key]
 * </pre>
 */
public class AliyunOSSUri {
    protected final String accessKeyId;
    protected final String secretAccessKey;
    protected final String bucketName;
    protected final String endpoint;
    protected final String key;
    private volatile String uri;

    public AliyunOSSUri(String uri) {
        this(URI.create(uri));
    }

    public AliyunOSSUri(URI uri) {
        Objects.requireNonNull(uri.getHost());
        Objects.requireNonNull(uri.getPath());
        if (uri.getUserInfo() != null) {
            String[] userInfo = uri.getUserInfo().split(":", 2);
            accessKeyId = userInfo[0];
            secretAccessKey = userInfo[1];
        } else {
            accessKeyId = null;
            secretAccessKey = null;
        }
        String[] host = uri.getHost().split("\\.", 2);
        if (host.length == 1) {
            bucketName = host[0];
            endpoint = null;
        } else {
            bucketName = host[0];
            endpoint = host[1];
        }
        key = uri.getPath().isEmpty() ? "" : uri.getPath().substring(1);
    }

    public AliyunOSSUri(String accessKeyId, String secretAccessKey, String bucketName, String endpoint, String key) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        this.key = key;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKeyId, secretAccessKey, bucketName, endpoint, key);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AliyunOSSUri) {
            AliyunOSSUri that = (AliyunOSSUri) o;
            return Objects.equals(this.accessKeyId, that.accessKeyId) &&
                    Objects.equals(this.secretAccessKey, that.secretAccessKey) &&
                    Objects.equals(this.bucketName, that.bucketName) &&
                    Objects.equals(this.endpoint, that.endpoint) &&
                    Objects.equals(this.key, that.key);
        }
        return false;
    }

    @Override
    public String toString() {
        if (uri == null) {
            if (bucketName == null && endpoint == null) {
                return key;
            }

            StringBuilder buffer = new StringBuilder();
            buffer.append("oss://");
            if (accessKeyId != null && secretAccessKey != null) {
                buffer.append(accessKeyId).append(':').append(secretAccessKey).append('@');
            }
            if (bucketName != null) {
                buffer.append(bucketName);
            }
            if (bucketName != null && endpoint != null) {
                buffer.append('.');
            }
            if (endpoint != null) {
                buffer.append(endpoint);
            }
            if (key != null) {
                buffer.append('/').append(key);
            }
            uri = buffer.toString();
        }
        return uri;
    }
}
