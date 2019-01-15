package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.aliyun.oss.vfs.AliyunOSSFileObject;
import cc.whohow.vfs.operations.FileOperationFactoryProvider;
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
        return fileObject.getURL(expiration);
    }

    @Override
    public void process() throws FileSystemException {
        get();
    }

    public static class Provider extends FileOperationFactoryProvider<AliyunOSSFileObject, GetSignedUrl> {
        public Provider() {
            super(AliyunOSSFileObject.class, GetSignedUrl.class, AliyunOSSGetSignedUrl::new);
        }
    }
}
