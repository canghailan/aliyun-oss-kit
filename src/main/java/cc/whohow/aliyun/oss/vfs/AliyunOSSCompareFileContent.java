package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.CompareFileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

public class AliyunOSSCompareFileContent implements CompareFileContent {
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
        try {
            Object eTag1 = fileObject.getContent().getAttribute("ETag");
            Object eTag2 = fileObjectForCompare.getContent().getAttribute("ETag");
            if (eTag1 == null || eTag2 == null) {
                return false;
            }
            return eTag1.equals(eTag2);
        } catch (FileSystemException e) {
            return false;
        }
    }

    @Override
    public boolean isDifferent() {
        try {
            Object eTag1 = fileObject.getContent().getAttribute("ETag");
            Object eTag2 = fileObjectForCompare.getContent().getAttribute("ETag");
            if (eTag1 == null || eTag2 == null) {
                return false;
            }
            return !eTag1.equals(eTag2);
        } catch (FileSystemException e) {
            return false;
        }
    }

    @Override
    public void process() {
    }
}
