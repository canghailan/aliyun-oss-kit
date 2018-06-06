package cc.whohow.aliyun.oss.vfs.operations;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

import java.lang.reflect.UndeclaredThrowableException;

public interface CompareFileContent extends FileOperation {
    static CompareFileContent apply(FileObject fileObject) {
        try {
            return (CompareFileContent) fileObject.getFileOperations().getOperation(CompareFileContent.class);
        } catch (FileSystemException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    CompareFileContent setFileObjectForCompare(FileObject fileObject);

    /**
     * 返回true一定相同，返回false不一定相同（可能相同）
     */
    boolean isIdentical();

    /**
     * 返回true一定不同，返回false不一定不同（可能不同）
     */
    boolean isDifferent();
}
