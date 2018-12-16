package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

/**
 * 阿里云 OSS 全局配置及缓存
 */
public class AliyunOSS {
    private static final Set<AliyunOSSUri> PROFILES = new CopyOnWriteArraySet<>();
    private static final AliyunOSSContext CONTEXT = new AliyunOSSContext();
    private static final AliyunOSSUrlFactory URL_FACTORY = new AliyunOSSUrlFactory(CONTEXT);
    private static volatile ScheduledExecutorService EXECUTOR;

    /**
     * 新增配置
     */
    public static void configure(String accessKeyId, String secretAccessKey) {
        AliyunOSSUri profile = new AliyunOSSUri(
                accessKeyId, secretAccessKey, null, null, null);
        if (PROFILES.add(profile)) {
            CONTEXT.addProfile(profile);
        }
    }

    /**
     * 新增配置
     */
    public static void configure(AliyunOSSUri uri) {
        if (uri.getAccessKeyId() != null && uri.getSecretAccessKey() != null) {
            configure(uri.getAccessKeyId(), uri.getSecretAccessKey());
        }
    }

    /**
     * 配置Cname
     */
    public static void configureCname(String uri, String cname) {
        configureCname(new AliyunOSSUri(uri), URI.create(cname));
    }

    /**
     * 配置Cname
     */
    public static void configureCname(AliyunOSSUri uri, URI cname) {
        URL_FACTORY.configureCname(uri, cname);
    }


    public static AliyunOSSContext getContext() {
        return CONTEXT;
    }

    public static AliyunOSSUrlFactory getUrlFactory() {
        return URL_FACTORY;
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
     * 关闭并回收资源
     */
    public static void shutdown() {
        shutdownSafety(EXECUTOR);
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
        return URL_FACTORY.getExtranetUrl(uri);
    }

    public static String getIntranetUrl(AliyunOSSUri uri) {
        return URL_FACTORY.getIntranetUrl(uri);
    }

    public static String getCnameUrl(AliyunOSSUri uri) {
        return URL_FACTORY.getCnameUrl(uri);
    }

    public static String getCnameUrl(AliyunOSSUri uri, Date expires) {
        return URL_FACTORY.getCnameUrl(uri, expires);
    }

    public static String getUrl(AliyunOSSUri uri) {
        return URL_FACTORY.getUrl(uri);
    }

    public static ScheduledExecutorService getExecutor() {
        return EXECUTOR;
    }

    public static synchronized void setExecutor(ScheduledExecutorService executorService) {
        if (EXECUTOR != null) {
            throw new IllegalStateException();
        }
        EXECUTOR = executorService;
    }
}
