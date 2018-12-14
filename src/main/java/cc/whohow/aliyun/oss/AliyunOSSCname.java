package cc.whohow.aliyun.oss;

import org.apache.commons.codec.digest.DigestUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class AliyunOSSCname {
    private final NavigableMap<String, URI> cnames = new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    /**
     * 配置Cname
     */
    public void configureCname(AliyunOSSUri uri, String cname) {
        configureCname(uri, URI.create(cname));
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
}
