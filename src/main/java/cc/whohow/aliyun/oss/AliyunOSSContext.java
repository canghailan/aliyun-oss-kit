package cc.whohow.aliyun.oss;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.Bucket;
import com.aliyuncs.auth.AlibabaCloudCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class AliyunOSSContext implements Closeable {
    protected final List<IClientProfile> profiles = new CopyOnWriteArrayList<>();
    protected final ClientConfiguration clientConfiguration;
    protected final Map<AliyunOSSUri, OSS> oss = new ConcurrentHashMap<>();
    protected final Map<String, BucketContext> buckets = new ConcurrentHashMap<>();

    public AliyunOSSContext() {
        this(new ClientConfiguration());
    }

    public AliyunOSSContext(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    public void addProfile(AliyunOSSUri profile) {
        Objects.requireNonNull(profile.getAccessKeyId());
        Objects.requireNonNull(profile.getSecretAccessKey());
        addProfile(DefaultProfile.getProfile(null, profile.getAccessKeyId(), profile.getSecretAccessKey()));
    }

    public void addProfile(IClientProfile profile) {
        this.profiles.add(profile);
        listBuckets(profile).forEach(this::addBucket);
    }

    public List<Bucket> listBuckets() {
        return buckets.values().stream()
                .map(bucketContext -> bucketContext.bucket)
                .collect(Collectors.toList());
    }

    public Bucket getBucket(String name) {
        BucketContext bucketContext = buckets.get(name);
        Objects.requireNonNull(bucketContext);
        return bucketContext.bucket;
    }

    public OSS getOSS(String name) {
        BucketContext bucketContext = buckets.get(name);
        Objects.requireNonNull(bucketContext);
        return bucketContext.oss;
    }

    public OSS getOSS(AliyunOSSUri uri) {
        if (uri.getAccessKeyId() != null && uri.getSecretAccessKey() != null && uri.getEndpoint() != null) {
            return oss.computeIfAbsent(new AliyunOSSUri(
                    uri.getAccessKeyId(),
                    uri.getSecretAccessKey(),
                    null,
                    AliyunOSSEndpointsProvider.getEndpoint(uri.getEndpoint()),
                    null), this::newOSS);
        }
        if (uri.getBucketName() != null) {
            return getOSS(uri.getBucketName());
        }
        throw new IllegalArgumentException(uri.toString());
    }

    private OSS newOSS(AliyunOSSUri uri) {
        return new OSSLoggingProxy(new OSSClient(
                uri.getEndpoint(),
                new DefaultCredentialProvider(uri.getAccessKeyId(), uri.getSecretAccessKey()),
                clientConfiguration));
    }

    private List<BucketContext> listBuckets(IClientProfile profile) {
        AlibabaCloudCredentials credentials = new StaticCredentialsProvider(profile).getCredentials();
        CredentialsProvider credentialsProvider = new DefaultCredentialProvider(credentials.getAccessKeyId(), credentials.getAccessKeySecret());
        OSS oss = new OSSClient(AliyunOSSEndpointsProvider.getDefaultEndpoint(), credentialsProvider, clientConfiguration);
        try {
            List<Bucket> bucketList = oss.listBuckets();
            List<BucketContext> bucketContextList = new ArrayList<>(bucketList.size());
            for (Bucket bucket : bucketList) {
                AliyunOSSUri uri = new AliyunOSSUri(
                        credentials.getAccessKeyId(),
                        credentials.getAccessKeySecret(),
                        bucket.getName(),
                        AliyunOSSEndpointsProvider.getEndpoint(bucket.getExtranetEndpoint()),
                        null);
                bucketContextList.add(new BucketContext(bucket, profile, getOSS(uri), uri));
            }
            return bucketContextList;
        } finally {
            close(oss);
        }
    }

    private void addBucket(BucketContext bucketContext) {
        buckets.put(bucketContext.bucket.getName(), bucketContext);
    }

    @Override
    public void close() {
        oss.values().forEach(this::close);
    }

    private void close(OSS oss) {
        if (oss != null) {
            try {
                oss.shutdown();
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public String toString() {
        return buckets.values().stream()
                .map(BucketContext::toString)
                .sorted()
                .collect(Collectors.joining("\n"));
    }

    static class BucketContext {
        final Bucket bucket;
        final IClientProfile profile;
        final OSS oss;
        final AliyunOSSUri uri;

        BucketContext(Bucket bucket, IClientProfile profile, OSS oss, AliyunOSSUri uri) {
            this.bucket = bucket;
            this.profile = profile;
            this.oss = oss;
            this.uri = uri;
        }

        @Override
        public String toString() {
            return uri.toString();
        }
    }
}
