package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.Bucket;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

/**
 * 阿里云 OSS 全局配置及缓存
 */
public class AliyunOSS {
    private static final Set<AliyunOSSUri> PROFILES = new ConcurrentSkipListSet<>();
    private static final AliyunOSSContext CONTEXT = new AliyunOSSContext();
    private static final AliyunOSSCname CNAME = new AliyunOSSCname();
    /**
     * 线程池
     */
    private static volatile ScheduledExecutorService executor;

    /**
     * 新增配置
     */
    public static void configure(AliyunOSSUri uri) {
        AliyunOSSUri profile = new AliyunOSSUri(
                uri.getAccessKeyId(), uri.getSecretAccessKey(), null, null, null);
        if (PROFILES.add(profile)) {
            CONTEXT.addProfile(profile);
        }
    }

    /**
     * 获取Bucket对应OSS客户端
     */
    public static OSS getOSS(String bucketName) {
        return CONTEXT.getOSS(bucketName);
    }

    /**
     * 获取OSS客户端
     */
    public static OSS getOSS(AliyunOSSUri uri) {
        return CONTEXT.getOSS(uri);
    }

    /**
     * 获取OSS对象
     */
    public static AliyunOSSObject getAliyunOSSObject(String uri) {
        return getAliyunOSSObject(new AliyunOSSUri(uri));
    }

    /**
     * 获取OSS对象
     */
    public static AliyunOSSObject getAliyunOSSObject(AliyunOSSUri uri) {
        return new AliyunOSSObject(getOSS(uri), uri.getBucketName(), uri.getKey());
    }

    /**
     * 获取OSS对象
     */
    public static AliyunOSSObjectAsync getAliyunOSSObjectAsync(String uri) {
        return getAliyunOSSObjectAsync(new AliyunOSSUri(uri));
    }

    /**
     * 获取OSS对象
     */
    public static AliyunOSSObjectAsync getAliyunOSSObjectAsync(AliyunOSSUri uri) {
        return new AliyunOSSObjectAsync(getOSS(uri), uri.getBucketName(), uri.getKey(), executor);
    }

    /**
     * 关闭并回收资源
     */
    public static void shutdown() {
        shutdownSafety(executor);
        CONTEXT.close();
    }

    /**
     * 安全关闭客户端
     */
    private static void shutdownSafety(ScheduledExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdownNow();
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            }
        }
    }

    public static String getExtranetUrl(AliyunOSSUri uri) {
        Bucket bucket = CONTEXT.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucket.getExtranetEndpoint() + "/" + uri.getKey();
    }

    public static String getIntranetUrl(AliyunOSSUri uri) {
        Bucket bucket = CONTEXT.getBucket(uri.getBucketName());
        if (bucket == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucket.getIntranetEndpoint() + "/" + uri.getKey();
    }

    public static String getCnameUrl(AliyunOSSUri uri) {
        return CNAME.getCnameUrl(uri);
    }

    public static String getCnameUrl(AliyunOSSUri uri, Date expires) {
        return CNAME.getCnameUrl(uri, expires);
    }

    public static String getUrl(AliyunOSSUri uri) {
        String cnameUrl = getCnameUrl(uri);
        if (cnameUrl != null) {
            return cnameUrl;
        }
        return getExtranetUrl(uri);
    }

    /**
     * 配置Cname
     */
    public static void configureCname(AliyunOSSUri uri, String cname) {
        CNAME.configureCname(uri, cname);
    }

    /**
     * 配置Cname
     */
    public static void configureCname(AliyunOSSUri uri, URI cname) {
        CNAME.configureCname(uri, cname);
    }

    public static ScheduledExecutorService getExecutor() {
        return executor;
    }

    public static synchronized void setExecutor(ScheduledExecutorService executorService) {
        if (executor != null) {
            throw new IllegalStateException();
        }
        executor = executorService;
    }
}
