package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.GetSignedUrl;
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
        return fileObject.getURL().toString();
    }

    @Override
    public void process() throws FileSystemException {

    }
}
