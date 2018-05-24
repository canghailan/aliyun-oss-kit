package cc.whohow.aliyun.oss.vfs;

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
        return fileObject.getOSS()
                .generatePresignedUrl(fileObject.getBucketName(), fileObject.getKey(), expiration)
                .toString();
    }

    @Override
    public void process() throws FileSystemException {
        get();
    }
}
