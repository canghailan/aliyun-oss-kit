package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;

import java.util.function.Function;
import java.util.function.Supplier;

public class AliyunOSSSupplier implements Supplier<OSS>, Function<AliyunOSSUri, OSS>, AutoCloseable {
    private final OSS oss;

    public AliyunOSSSupplier(OSS oss) {
        this.oss = oss;
    }

    public AliyunOSSSupplier(AliyunOSSUri uri) {
        this(uri, new AliyunOSSFactory());
    }

    public AliyunOSSSupplier(AliyunOSSUri uri, Function<AliyunOSSUri, OSS> ossFactory) {
        this(ossFactory.apply(uri));
    }

    public AliyunOSSSupplier(String uri) {
        this(new AliyunOSSUri(uri));
    }

    public AliyunOSSSupplier(String accessKeyId, String secretAccessKey, String endpoint) {
        this(new AliyunOSSUri(accessKeyId, secretAccessKey, null, endpoint, null));
    }

    @Override
    public OSS apply(AliyunOSSUri uri) {
        return get();
    }

    @Override
    public OSS get() {
        return oss;
    }

    @Override
    public void close() {
        oss.shutdown();
    }
}
