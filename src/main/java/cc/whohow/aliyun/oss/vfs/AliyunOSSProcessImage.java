package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

class AliyunOSSProcessImage implements ProcessImage {
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
                AliyunOSS.getExtranetUrl(originFileObject.getName()) + parameters);
    }
}
