package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.aliyun.oss.vfs.AliyunOSSFileObject;
import cc.whohow.vfs.operations.FileOperationFactoryProvider;
import org.apache.commons.vfs2.FileSystemException;

public class AliyunOSSGetAccountAlias implements GetAccountAlias {
    private AliyunOSSFileObject fileObject;

    public AliyunOSSGetAccountAlias(AliyunOSSFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public String get() {
        return fileObject.getFileSystem().getAccountAlias();
    }

    @Override
    public void process() throws FileSystemException {

    }

    public static class Provider extends FileOperationFactoryProvider<AliyunOSSFileObject, GetAccountAlias> {
        public Provider() {
            super(AliyunOSSFileObject.class, GetAccountAlias.class, AliyunOSSGetAccountAlias::new);
        }
    }
}
