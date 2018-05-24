package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.GetSignedUrl;
import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;
import org.apache.commons.vfs2.operations.FileOperations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class AliyunOSSFileOperations implements FileOperations {
    private static final Map<Class<? extends FileOperation>, Function<? super AliyunOSSFileObject, ? extends FileOperation>> FACTORY = new ConcurrentHashMap<>();

    static {
        FACTORY.put(ProcessImage.class, AliyunOSSProcessImage::new);
        FACTORY.put(GetSignedUrl.class, AliyunOSSGetSignedUrl::new);
    }

    private final AliyunOSSFileObject fileObject;

    public AliyunOSSFileOperations(AliyunOSSFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends FileOperation>[] getOperations() throws FileSystemException {
        return FACTORY.keySet().toArray(new Class[0]);
    }

    @Override
    public FileOperation getOperation(Class<? extends FileOperation> operationClass) throws FileSystemException {
        Function<? super AliyunOSSFileObject, ? extends FileOperation> factory = FACTORY.get(operationClass);
        if (factory != null) {
            return factory.apply(fileObject);
        }
        throw new FileSystemException("");
    }

    @Override
    public boolean hasOperation(Class<? extends FileOperation> operationClass) throws FileSystemException {
        return FACTORY.containsKey(operationClass);
    }
}
