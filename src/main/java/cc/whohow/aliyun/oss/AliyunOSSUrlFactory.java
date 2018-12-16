package cc.whohow.aliyun.oss;

import com.aliyun.oss.model.Bucket;
import org.apache.commons.codec.digest.DigestUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class AliyunOSSUrlFactory {
    private final AliyunOSSContext context;
    private final NavigableMap<String, URI> cnames = new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    public AliyunOSSUrlFactory(AliyunOSSContext context) {
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
        cnames.put("oss://" + uri.getBucketName() + "/" + uri.getKey(), cname);
    }

    public Map<String, URI> getCnames() {
        return Collections.unmodifiableMap(cnames);
    }

    public String getCnameUrl(AliyunOSSUri uri) {
        return getCnameUrl(uri, null);
    }

    public String getCnameUrl(AliyunOSSUri uri, Date expires) {
        String key = "oss://" + uri.getBucketName() + "/" + uri.getKey();
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

    public Collection<String> getUrls(AliyunOSSUri uri) {
        Set<String> urls = new HashSet<>(8);
        String intranetUrl = AliyunOSS.getIntranetUrl(uri);
        String extranetUrl = AliyunOSS.getExtranetUrl(uri);
        String cnameUrl = AliyunOSS.getCnameUrl(uri);
        urls.add(intranetUrl);
        urls.add(http(intranetUrl));
        urls.add(extranetUrl);
        urls.add(http(extranetUrl));
        urls.add(cnameUrl);
        urls.add(http(cnameUrl));
        urls.remove(null);
        return urls;
    }
}
