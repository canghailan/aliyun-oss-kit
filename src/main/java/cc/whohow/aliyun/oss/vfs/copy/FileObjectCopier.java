package cc.whohow.aliyun.oss.vfs.copy;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperation;

import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 文件拷贝工具
 * @see cc.whohow.vfs.FluentFileObject
 */
@Deprecated
public class FileObjectCopier implements Callable<FileObject> {
    protected FileObject source;
    protected FileObject target;
    protected FileSelector selector = Selectors.SELECT_ALL;

    protected FileObject resolve(String uri) {
        try {
            return VFS.getManager().resolveFile(uri);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileObjectCopier setSource(String source) {
        this.source = resolve(source);
        return this;
    }

    public FileObjectCopier setSource(FileObject source) {
        this.source = source;
        return this;
    }

    public FileObjectCopier setTarget(String target) {
        this.target = resolve(target);
        return this;
    }

    public FileObjectCopier setTarget(FileObject target) {
        this.target = target;
        return this;
    }

    public FileObjectCopier withSource(Function<FileObject, FileObject> function) {
        this.source = function.apply(source);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <OP extends FileOperation> FileObjectCopier withSourceOperation(Class<OP> operation,
                                                                           BiFunction<FileObject, OP, FileObject> function) {
        try {
            this.source = function.apply(source, (OP) source.getFileOperations().getOperation(operation));
            return this;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileObjectCopier withTarget(BiFunction<FileObject, FileObject, FileObject> function) {
        this.target = function.apply(source, target);
        return this;
    }

    public FileObjectCopier withRandomTarget() {
        return withTarget(this::newRandomFileObject);
    }

    @Override
    public FileObject call() {
        try {
            target.copyFrom(source, selector);
            return target;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        } finally {
            close();
        }
    }

    protected void close() {
    }

    protected String newRandomName(FileObject fileObject) {
        String extension = fileObject.getName().getExtension();
        if (extension.isEmpty()) {
            return UUID.randomUUID().toString();
        }
        return UUID.randomUUID() + "." + extension;
    }

    protected FileObject newRandomFileObject(FileObject source, FileObject targetDirectory) {
        try {
            if (targetDirectory.isFolder()) {
                return targetDirectory.getChild(newRandomName(source));
            }
            throw new IllegalStateException();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }
}
