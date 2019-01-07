package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.aliyun.oss.vfs.AliyunOSSFileObject;
import cc.whohow.vfs.operations.FileOperationFactoryProvider;
import com.aliyun.oss.model.Bucket;
import org.apache.commons.vfs2.FileSystemException;

public class AliyunOSSGetBucket implements GetBucket {
    private AliyunOSSFileObject fileObject;

    public AliyunOSSGetBucket(AliyunOSSFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public Bucket get() {
        return fileObject.getFileSystem().getBucket();
    }

    @Override
    public void process() throws FileSystemException {

    }

    public static class Provider extends FileOperationFactoryProvider<AliyunOSSFileObject, GetBucket> {
        public Provider() {
            super(AliyunOSSFileObject.class, GetBucket.class, AliyunOSSGetBucket::new);
        }
    }
}
