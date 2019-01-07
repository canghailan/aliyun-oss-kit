package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;

import java.util.function.Function;

public class AliyunOSSObjectFactory implements Function<AliyunOSSUri, AliyunOSSObject> {
    private final Function<AliyunOSSUri, OSS> ossFactory;

    public AliyunOSSObjectFactory(Function<AliyunOSSUri, OSS> ossFactory) {
        this.ossFactory = ossFactory;
    }

    public AliyunOSSObject apply(String uri) {
        return apply(new AliyunOSSUri(uri));
    }

    @Override
    public AliyunOSSObject apply(AliyunOSSUri uri) {
        return new AliyunOSSObject(ossFactory.apply(uri), uri.getBucketName(), uri.getKey());
    }
}
