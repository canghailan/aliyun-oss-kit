package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.vfs.FileObjectFns;
import cc.whohow.vfs.operations.FileOperationFactoryProvider;
import cc.whohow.vfs.provider.uri.UriFileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.util.Date;

public class UriGetSignedUrl implements GetSignedUrl {
    private final UriFileObject fileObject;

    public UriGetSignedUrl(UriFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public GetSignedUrl setExpiration(Date expiration) {
        return this;
    }

    @Override
    public String get() {
        return FileObjectFns.getURL(fileObject).toString();
    }

    @Override
    public void process() throws FileSystemException {

    }

    public static class Provider extends FileOperationFactoryProvider<UriFileObject, GetSignedUrl> {
        public Provider() {
            super(UriFileObject.class, GetSignedUrl.class, UriGetSignedUrl::new);
        }
    }
}
