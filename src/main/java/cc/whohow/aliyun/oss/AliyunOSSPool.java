package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class AliyunOSSPool implements Function<AliyunOSSUri, OSS>, AutoCloseable {
    private final Function<AliyunOSSUri, OSS> factory;
    private final Map<AliyunOSSUri, OSS> pool = new ConcurrentHashMap<>();

    public AliyunOSSPool(Function<AliyunOSSUri, OSS> factory) {
        this.factory = factory;
    }

    @Override
    public OSS apply(AliyunOSSUri uri) {
        return pool.computeIfAbsent(uri, factory);
    }

    @Override
    public void close() throws Exception {
        pool.values().forEach(this::close);
    }

    private void close(OSS oss) {
        oss.shutdown();
    }
}
