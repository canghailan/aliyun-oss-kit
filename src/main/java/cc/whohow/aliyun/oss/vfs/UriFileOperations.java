package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;
import org.apache.commons.vfs2.operations.FileOperations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class UriFileOperations implements FileOperations {
    private static final Map<Class<? extends FileOperation>, Function<? super UriFileObject, ? extends FileOperation>> FACTORY = new ConcurrentHashMap<>();

    static {
        FACTORY.put(ProcessImage.class, UriProcessImage::new);
    }

    private final UriFileObject fileObject;

    public UriFileOperations(UriFileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends FileOperation>[] getOperations() throws FileSystemException {
        return FACTORY.keySet().toArray(new Class[0]);
    }

    @Override
    public FileOperation getOperation(Class<? extends FileOperation> operationClass) throws FileSystemException {
        Function<? super UriFileObject, ? extends FileOperation> factory = FACTORY.get(operationClass);
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
