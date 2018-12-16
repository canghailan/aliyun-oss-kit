package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.aliyun.oss.vfs.AliyunOSSFileObject;
import cc.whohow.aliyun.oss.vfs.AliyunOSSFileVersionProvider;
import cc.whohow.vfs.version.FileVersion;
import org.apache.commons.vfs2.FileObject;

public class AliyunOSSCompareFileContent implements CompareFileContent {
    private AliyunOSSFileVersionProvider fileVersionProvider = new AliyunOSSFileVersionProvider();
    private AliyunOSSFileObject fileObject;
    private FileObject fileObjectForCompare;

    public AliyunOSSCompareFileContent(AliyunOSSFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public CompareFileContent setFileObjectForCompare(FileObject fileObject) {
        this.fileObjectForCompare = fileObject;
        return this;
    }

    @Override
    public boolean isIdentical() {
        FileVersion<?> v1 = fileVersionProvider.getVersion(fileObject);
        FileVersion<?> v2 = fileVersionProvider.getVersion(fileObjectForCompare);
        if (v1.getVersion() == null || v2.getVersion() == null) {
            return false;
        }
        return v1.getVersion().equals(v2.getVersion());
    }

    @Override
    public boolean isDifferent() {
        FileVersion<?> v1 = fileVersionProvider.getVersion(fileObject);
        FileVersion<?> v2 = fileVersionProvider.getVersion(fileObjectForCompare);
        if (v1.getVersion() == null || v2.getVersion() == null) {
            return false;
        }
        return !v1.getVersion().equals(v2.getVersion());
    }

    @Override
    public void process() {
    }
}
