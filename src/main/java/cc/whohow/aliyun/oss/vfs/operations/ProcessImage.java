package cc.whohow.aliyun.oss.vfs.operations;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.function.Supplier;

public interface ProcessImage extends FileOperation, Supplier<FileObject> {
    static ProcessImage apply(FileObject fileObject) {
        try {
            return (ProcessImage) fileObject.getFileOperations().getOperation(ProcessImage.class);
        } catch (FileSystemException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    ProcessImage setParameters(String parameters);

    FileObject getOriginFileObject();

    FileObject getProcessedFileObject();

    @Override
    default FileObject get() {
        try {
            process();
            return getProcessedFileObject();
        } catch (FileSystemException e) {
            throw new UndeclaredThrowableException(e);
        }
    }
}
