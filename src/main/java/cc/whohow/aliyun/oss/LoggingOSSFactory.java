package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;

import java.util.function.Function;

public class LoggingOSSFactory implements Function<AliyunOSSUri, OSS> {
    private final Function<AliyunOSSUri, OSS> factory;

    public LoggingOSSFactory(Function<AliyunOSSUri, OSS> factory) {
        this.factory = factory;
    }

    @Override
    public OSS apply(AliyunOSSUri uri) {
        return new LoggingOSSProxy(factory.apply(uri));
    }
}
