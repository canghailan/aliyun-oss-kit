package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.vfs.operations.GetSignedUrl;
import org.apache.commons.vfs2.FileSystemException;

import java.util.Date;

public class AliyunOSSGetSignedUrl implements GetSignedUrl {
    private AliyunOSSFileObject fileObject;
    private Date expiration;

    public AliyunOSSGetSignedUrl(AliyunOSSFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public GetSignedUrl setExpiration(Date expiration) {
        this.expiration = expiration;
        return this;
    }

    @Override
    public String get() {
        String url = AliyunOSS.getUrl(fileObject.getName());
        String presignedUrl = fileObject.getOSS()
                .generatePresignedUrl(fileObject.getBucketName(), fileObject.getKey(), expiration)
                .toString();
        int index = presignedUrl.indexOf('?');
        if (index < 0) {
            return url;
        }
        return url + presignedUrl.substring(index);
    }

    @Override
    public void process() throws FileSystemException {
        get();
    }
}
