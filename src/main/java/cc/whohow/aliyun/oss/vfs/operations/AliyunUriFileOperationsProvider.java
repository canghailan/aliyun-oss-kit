package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.vfs.operations.FileOperationsProvider;

public class AliyunUriFileOperationsProvider extends FileOperationsProvider {
    public AliyunUriFileOperationsProvider() {
        super(UriProcessImage.class,
                UriGetSignedUrl.class);
    }
}
