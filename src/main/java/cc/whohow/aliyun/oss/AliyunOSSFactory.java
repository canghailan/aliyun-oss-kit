package cc.whohow.aliyun.oss;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;

import java.util.function.Function;

public class AliyunOSSFactory implements Function<AliyunOSSUri, OSS> {
    private final ClientConfiguration clientConfiguration;

    public AliyunOSSFactory() {
        this(new ClientConfiguration());
    }

    public AliyunOSSFactory(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    @Override
    public OSS apply(AliyunOSSUri uri) {
        if (uri.getAccessKeyId() == null || uri.getSecretAccessKey() == null || uri.getEndpoint() == null) {
            throw new IllegalArgumentException(uri.toString());
        }
        return new OSSClient(
                AliyunOSSEndpoints.getEndpoint(uri.getEndpoint()),
                new DefaultCredentialProvider(uri.getAccessKeyId(), uri.getSecretAccessKey()),
                clientConfiguration);
    }
}
