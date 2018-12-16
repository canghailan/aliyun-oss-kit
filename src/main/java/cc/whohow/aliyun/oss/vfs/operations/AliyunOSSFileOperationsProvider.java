package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.vfs.operations.FileOperationsProvider;

public class AliyunOSSFileOperationsProvider extends FileOperationsProvider {
    public AliyunOSSFileOperationsProvider() {
        super(AliyunOSSProcessImage.class,
                AliyunOSSGetSignedUrl.class,
                AliyunOSSCompareFileContent.class);
    }
}
