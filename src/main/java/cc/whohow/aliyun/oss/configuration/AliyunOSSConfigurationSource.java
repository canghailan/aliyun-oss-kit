package cc.whohow.aliyun.oss.configuration;

import cc.whohow.aliyun.oss.AliyunOSSObject;
import cc.whohow.aliyun.oss.io.Java9InputStream;
import cc.whohow.configuration.provider.AbstractFileBasedConfigurationSource;

import java.io.IOException;

public class AliyunOSSConfigurationSource extends AbstractFileBasedConfigurationSource {
    private final AliyunOSSObject object;

    public AliyunOSSConfigurationSource(AliyunOSSObject object) {
        this.object = object;
    }

    @Override
    public byte[] load() throws IOException {
        try (Java9InputStream stream = new Java9InputStream(object.getObjectContent())) {
            return stream.readAllBytes();
        }
    }

    @Override
    public void close() throws IOException {
    }
}
