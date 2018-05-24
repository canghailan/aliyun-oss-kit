package cc.whohow.aliyun.oss.vfs.operations;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.operations.FileOperation;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.Date;
import java.util.function.Supplier;

public interface GetSignedUrl extends FileOperation, Supplier<String> {
    GetSignedUrl setExpiration(Date expiration);

    default GetSignedUrl setExpiresIn(Duration expiresIn) {
        return setExpiration(new Date(System.currentTimeMillis() * expiresIn.toMillis()));
    }

    static GetSignedUrl apply(FileObject fileObject) {
        try {
            return (GetSignedUrl) fileObject.getFileOperations().getOperation(GetSignedUrl.class);
        } catch (FileSystemException e) {
            throw new UndeclaredThrowableException(e);
        }
    }
}
