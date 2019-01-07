package cc.whohow.aliyun.oss.vfs.operations;

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
                originFileObject.getFileSystem().getFileProvider().getUriFactory()
                        .getExtranetUrl(originFileObject.getName()) + parameters);
    }

    public static class Provider extends FileOperationFactoryProvider<AliyunOSSFileObject, ProcessImage> {
        public Provider() {
            super(AliyunOSSFileObject.class, ProcessImage.class, AliyunOSSProcessImage::new);
        }
    }
}
