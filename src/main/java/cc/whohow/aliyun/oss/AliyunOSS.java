package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.net.Ping;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.BucketInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 阿里云 OSS 全局配置及缓存
 * <a href="https://www.alibabacloud.com/help/zh/doc-detail/31837.htm">OSS开通Region和Endpoint对照表</a>
 */
public class AliyunOSS {
    /**
     * Bucket名称 <-> 标准 Bucket URI
     */
    private static final Map<String, AliyunOSSUri> CONFIGURATIONS = new ConcurrentHashMap<>();
    /**
     * 标准 OSS URI <-> OSS
     */
    private static final Map<AliyunOSSUri, OSS> OSS_POOL = new ConcurrentHashMap<>();
    /**
     * Bucket名称 <-> OSS
     */
    private static final Map<String, OSS> OSS_CACHE = new ConcurrentHashMap<>();
    /**
     * Bucket名称 <-> BucketInfo
     */
    private static final Map<String, BucketInfo> BUCKET_CACHE = new ConcurrentHashMap<>();
    /**
     * 标准 Object URI <-> URL
     */
    private static final NavigableMap<String, String> CNAME = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    /**
     * 线程池
     */
    private static volatile ScheduledExecutorService executor;

    /**
     * 查询所有配置
     */
    public static Collection<AliyunOSSUri> getConfigurations() {
        return Collections.unmodifiableCollection(CONFIGURATIONS.values());
    }

    public static Map<String, String> getCnames() {
        return Collections.unmodifiableMap(CNAME);
    }

    /**
     * 新增配置
     */
    public static void configure(AliyunOSSUri uri) {
        if (!isConfigurable(uri) || isConfigured(uri)) {
            return;
        }
        synchronized (AliyunOSS.class) {
            if (isConfigured(uri)) {
                return;
            }
            // 临时 OSS URI
            AliyunOSSUri tempOSSUri = new AliyunOSSUri(
                    uri.getAccessKeyId(),
                    uri.getSecretAccessKey(),
                    null,
                    uri.getEndpoint(),
                    null);
            OSS oss = OSS_POOL.get(tempOSSUri);
            if (oss == null) {
                // 创建临时 OSS
                OSS tempOSS = new OSSClientBuilder().build(
                        tempOSSUri.getEndpoint(),
                        tempOSSUri.getAccessKeyId(),
                        tempOSSUri.getSecretAccessKey());

                // 读取 Bucket 信息
                BucketInfo bucketInfo = tempOSS.getBucketInfo(uri.getBucketName());
                // 缓存 Bucket 信息
                BUCKET_CACHE.put(bucketInfo.getBucket().getName(), bucketInfo);

                // 生成标准 OSS URI
                AliyunOSSUri ossUri = new AliyunOSSUri(
                        uri.getAccessKeyId(),
                        uri.getSecretAccessKey(),
                        null,
                        bucketInfo.getBucket().getExtranetEndpoint(),
                        null);
                // 重新查询OSS缓存
                oss = OSS_POOL.get(ossUri);
                if (oss == null) {
                    // 未缓存，优化临时OSS，并缓存
                    OSS_POOL.put(ossUri, optimizeOSS(tempOSS, tempOSSUri, bucketInfo));
                    OSS_CACHE.clear();
                } else {
                    // 已缓存，关闭临时OSS
                    shutdownSafety(tempOSS);
                }
            } else {
                // 读取 Bucket 信息
                BucketInfo bucketInfo = oss.getBucketInfo(uri.getBucketName());
                // 缓存 Bucket 信息
                BUCKET_CACHE.put(bucketInfo.getBucket().getName(), bucketInfo);
            }

            // 缓存标准配置信息
            AliyunOSSUri bucketUri = new AliyunOSSUri(
                    uri.getAccessKeyId(),
                    uri.getSecretAccessKey(),
                    uri.getBucketName(),
                    BUCKET_CACHE.get(uri.getBucketName()).getBucket().getExtranetEndpoint(),
                    null);
            CONFIGURATIONS.put(bucketUri.getBucketName(), bucketUri);
        }
    }

    /**
     * 获取Bucket对应OSS客户端
     */
    public static OSS getOSS(String bucketName) {
        return OSS_CACHE.computeIfAbsent(bucketName, AliyunOSS::getPooledOSS);
    }

    /**
     * 从OSS池中根据bucket获取OSS
     */
    private static OSS getPooledOSS(String bucketName) {
        AliyunOSSUri configuration = CONFIGURATIONS.get(bucketName);
        if (configuration == null) {
            return null;
        }
        AliyunOSSUri ossUri = new AliyunOSSUri(
                configuration.getAccessKeyId(),
                configuration.getSecretAccessKey(),
                null,
                configuration.getEndpoint(),
                null);
        return OSS_POOL.get(ossUri);
    }

    /**
     * 获取OSS客户端
     */
    public static OSS getOSS(AliyunOSSUri uri) {
        if (uri.getBucketName() == null) {
            AliyunOSSUri ossUri = new AliyunOSSUri(
                    uri.getAccessKeyId(),
                    uri.getSecretAccessKey(),
                    null,
                    uri.getEndpoint(),
                    null);
            return OSS_POOL.get(ossUri);
        }

        OSS oss = getOSS(uri.getBucketName());
        if (oss == null) {
            configure(uri);
            oss = getOSS(uri.getBucketName());
        }
        return oss;
    }

    /**
     * 获取Bucket信息
     */
    public static BucketInfo getBucketInfo(String bucketName) {
        return BUCKET_CACHE.get(bucketName);
    }

    /**
     * 获取Bucket信息
     */
    public static BucketInfo getBucketInfo(AliyunOSSUri uri) {
        BucketInfo bucket = BUCKET_CACHE.get(uri.getBucketName());
        if (bucket == null) {
            configure(uri);
            bucket = BUCKET_CACHE.get(uri.getBucketName());
        }
        return bucket;
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
        OSS_POOL.values().forEach(AliyunOSS::shutdownSafety);
    }

    /**
     * 安全关闭客户端
     */
    private static void shutdownSafety(OSS oss) {
        if (oss != null) {
            oss.shutdown();
        }
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

    /**
     * 是否可配置
     */
    private static boolean isConfigurable(AliyunOSSUri uri) {
        return uri.getAccessKeyId() != null &&
                uri.getSecretAccessKey() != null &&
                uri.getEndpoint() != null &&
                uri.getBucketName() != null;
    }

    /**
     * 是否已配置
     */
    private static boolean isConfigured(AliyunOSSUri uri) {
        AliyunOSSUri configured = CONFIGURATIONS.get(uri.getBucketName());
        if (configured == null) {
            return false;
        }
        return configured.getAccessKeyId().equals(uri.getAccessKeyId()) &&
                configured.getSecretAccessKey().equals(uri.getSecretAccessKey());
    }

    /**
     * 优化OSS，当探测到内网环境时，自动切换
     */
    private static OSS optimizeOSS(OSS tempOSS, AliyunOSSUri tempOSSUri, BucketInfo bucketInfo) {
        Bucket bucket = bucketInfo.getBucket();

        // 已经是内网环境客户端，不用优化
        if (tempOSSUri.getEndpoint().equals(bucket.getIntranetEndpoint())) {
            return tempOSS;
        }

        // 探测是否是内网环境
        if (Ping.ping(bucket.getIntranetEndpoint()) >= 0) {
            // 关闭客户端
            shutdownSafety(tempOSS);
            // 创建内网环境客户端
            return new OSSClientBuilder().build(
                    bucket.getIntranetEndpoint(),
                    tempOSSUri.getAccessKeyId(),
                    tempOSSUri.getSecretAccessKey());
        }

        return tempOSS;
    }

    /**
     * 是否是内网环境
     */
    public static boolean isIntranet() {
        return Ping.ping("oss-internal.aliyuncs.com") >= 0;
    }

    public static String getExtranetUrl(AliyunOSSUri uri) {
        BucketInfo bucketInfo = getBucketInfo(uri.getBucketName());
        if (bucketInfo == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucketInfo.getBucket().getExtranetEndpoint() + "/" + uri.getKey();
    }

    public static String getIntranetUrl(AliyunOSSUri uri) {
        BucketInfo bucketInfo = getBucketInfo(uri.getBucketName());
        if (bucketInfo == null) {
            throw new IllegalStateException();
        }
        return "https://" + uri.getBucketName() + "." + bucketInfo.getBucket().getIntranetEndpoint() + "/" + uri.getKey();
    }

    public static String getCnameUrl(AliyunOSSUri uri) {
        String key = "oss://" + uri.getBucketName() + "/" + uri.getKey();
        for (Map.Entry<String, String> e : CNAME.tailMap(key).entrySet()) {
            if (key.startsWith(e.getKey())) {
                return e.getValue() + key.substring(e.getKey().length());
            }
        }
        return null;
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
    public static void configureCname(AliyunOSSUri uri, String url) {
        Objects.requireNonNull(uri.getBucketName());
        Objects.requireNonNull(url);
        CNAME.put("oss://" + uri.getBucketName() + "/" + uri.getKey(), url);
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
