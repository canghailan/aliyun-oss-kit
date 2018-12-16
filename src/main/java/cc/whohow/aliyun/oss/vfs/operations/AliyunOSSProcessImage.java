package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.vfs.AliyunOSSFileObject;
import cc.whohow.vfs.operations.FileOperationFactoryProvider;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

public class AliyunOSSProcessImage implements ProcessImage {
    private AliyunOSSFileObject originFileObject;
    private FileObject processedFileObject;
    private String parameters = "";

    public AliyunOSSProcessImage(AliyunOSSFileObject originFileObject) {
        this.originFileObject = originFileObject;
    }

    public static FileOperationFactoryProvider<AliyunOSSFileObject, ProcessImage> provider() {
        return new FileOperationFactoryProvider<>(AliyunOSSFileObject.class, ProcessImage.class, AliyunOSSProcessImage::new);
    }

    @Override
    public ProcessImage setParameters(String parameters) {
        this.parameters = parameters;
        return this;
    }

    @Override
    public FileObject getOriginFileObject() {
        return originFileObject;
    }

    @Override
    public FileObject getProcessedFileObject() {
        return processedFileObject;
    }

    @Override
    public void process() throws FileSystemException {
        processedFileObject = originFileObject.getFileSystem().getFileSystemManager().resolveFile(
                AliyunOSS.getExtranetUrl(originFileObject.getName()) + parameters);
    }
}
