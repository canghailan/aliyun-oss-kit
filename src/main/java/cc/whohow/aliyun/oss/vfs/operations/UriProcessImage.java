package cc.whohow.aliyun.oss.vfs.operations;

import cc.whohow.vfs.operations.FileOperationFactoryProvider;
import cc.whohow.vfs.provider.uri.UriFileObject;
import org.apache.commons.vfs2.FileObject;

public class UriProcessImage implements ProcessImage {
    private final UriFileObject originFileObject;

    public UriProcessImage(UriFileObject originFileObject) {
        this.originFileObject = originFileObject;
    }

    public static FileOperationFactoryProvider<UriFileObject, ProcessImage> provider() {
        return new FileOperationFactoryProvider<>(UriFileObject.class, ProcessImage.class, UriProcessImage::new);
    }

    @Override
    public ProcessImage setParameters(String parameters) {
        return this;
    }

    @Override
    public FileObject getOriginFileObject() {
        return originFileObject;
    }

    @Override
    public FileObject getProcessedFileObject() {
        return originFileObject;
    }

    @Override
    public void process() {
        // do nothing
    }
}
