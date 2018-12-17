package cc.whohow.aliyun.oss;

import com.aliyun.oss.model.Bucket;
import org.apache.commons.codec.digest.DigestUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class AliyunOSSUriFactory {
    private final AliyunOSSContext context;
    private final NavigableMap<String, URI> cnames = new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    public AliyunOSSUriFactory(AliyunOSSContext context) {
        this.context = context;
    }

    private static String http(String https) {
        if (https == null) {
            return null;
        }
        return https.replaceFirst("^https://", "http://");
    }

    /**
     * 配置Cname
     */
    public void configureCname(AliyunOSSUri uri, URI cname) {
        Objects.requireNonNull(uri.getBucketName());
        Objects.requireNonNull(cname);
        cnames.put(getShortUri(uri), cname);
    }

    public Map<String, URI> getCnames() {
        return Collections.unmodifiableMap(cnames);
    }

    public String getCnameUrl(AliyunOSSUri uri) {
        return getCnameUrl(uri, null);
    }

    public String getCnameUrl(AliyunOSSUri uri, Date expires) {
        String key = getShortUri(uri);
        for (Map.Entry<String, URI> e : cnames.tailMap(key).entrySet()) {
            if (key.startsWith(e.getKey())) {
                URI cname = e.getValue();

                StringBuilder buffer = new StringBuilder(e.getKey().length() + uri.getKey().length());
                buffer.append(cname.getScheme()).append("://");
                buffer.append(cname.getHost());
                int mark = buffer.length();
                if (cname.getPath() == null || cname.getPath().isEmpty()) {
                    buffer.append("/");
                } else {
                    buffer.append(cname.getPath());
                }
                buffer.append(key, e.getKey().length(), key.length());
                if (expires == null || cname.getUserInfo() == null) {
                    return buffer.toString();
                }

                long timestamp = expires.getTime() / 1000L;
                String rand = UUID.randomUUID().toString().replace("-", "");
                int uid = 0;
                StringBuilder toSign = new StringBuilder(buffer.length() - mark + 100);
                toSign.append(buffer, mark, buffer.length()).append('-'); // URI-
                toSign.append(timestamp).append('-'); // Timestamp-
                toSign.append(rand).append('-'); // rand-
                toSign.append(uid).append('-'); // uid-
                toSign.append(cname.getUserInfo()); // PrivateKey
                String sign = DigestUtils.md5Hex(toSign.toString());

                return buffer.append("?auth_key=")
                        .append(timestamp).append('-')
                        .append(rand).append('-')
                        .append(uid).append('-')
                        .append(sign).toString();
            }
        }
        return null;
    }

    public String getExtranetUrl(AliyunOSSUri uri) {
        Bucket bucket = context.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucket.getExtranetEndpoint() + "/" + uri.getKey();
    }

    public String getIntranetUrl(AliyunOSSUri uri) {
        Bucket bucket = context.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucket.getIntranetEndpoint() + "/" + uri.getKey();
    }

    public String getUrl(AliyunOSSUri uri) {
        String cnameUrl = getCnameUrl(uri);
        if (cnameUrl != null) {
            return cnameUrl;
        }
        return getExtranetUrl(uri);
    }

    public String getShortUri(AliyunOSSUri uri) {
        return "oss://" + uri.getBucketName() + "/" + uri.getKey();
    }

    public String getLongExtranetUri(AliyunOSSUri uri) {
        Bucket bucket = context.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "oss://" + uri.getBucketName() + "." + bucket.getExtranetEndpoint() + "/" + uri.getKey();
    }

    public String getLongIntranetUri(AliyunOSSUri uri) {
        Bucket bucket = context.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "oss://" + uri.getBucketName() + "." + bucket.getIntranetEndpoint() + "/" + uri.getKey();
    }

    public Collection<String> getCanonicalUris(AliyunOSSUri uri) {
        Set<String> names = new HashSet<>(8);
        String shortUri = getShortUri(uri);
        String intranetUrl = getIntranetUrl(uri);
        String extranetUrl = getExtranetUrl(uri);
        String cnameUrl = getCnameUrl(uri);
        names.add(shortUri);
        names.add(getLongExtranetUri(uri));
        names.add(getLongIntranetUri(uri));
        names.add(intranetUrl);
        names.add(http(intranetUrl));
        names.add(extranetUrl);
        names.add(http(extranetUrl));
        names.add(cnameUrl);
        names.add(http(cnameUrl));
        names.remove(null);
        return names;
    }
}
