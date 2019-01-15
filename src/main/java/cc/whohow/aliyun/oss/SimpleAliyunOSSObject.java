package cc.whohow.aliyun.oss;

public class SimpleAliyunOSSObject extends AliyunOSSObject implements AutoCloseable {
    public SimpleAliyunOSSObject(String uri) {
        this(new AliyunOSSUri(uri));
    }

    public SimpleAliyunOSSObject(AliyunOSSUri uri) {
        super(new AliyunOSSFactory().apply(uri), uri.getBucketName(), uri.getKey());
    }

    @Override
    public void close() {
        oss.shutdown();
    }
}
