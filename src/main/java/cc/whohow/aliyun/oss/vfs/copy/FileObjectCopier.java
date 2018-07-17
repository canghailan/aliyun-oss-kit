package cc.whohow.aliyun.oss.vfs.copy;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperation;

import java.io.UncheckedIOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 文件拷贝工具
 */
public class FileObjectCopier implements Callable<FileObject> {
    private FileObject source;
    private FileObject target;
    private FileSelector selector = Selectors.SELECT_ALL;
    private IdentityHashMap<FileObject, Boolean> refs = new IdentityHashMap<>();

    public FileObject resolve(String uri) {
        try {
            return VFS.getManager().resolveFile(uri);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileObjectCopier setSource(String source) {
        this.source = resolve(source);
        refs.putIfAbsent(this.source, Boolean.TRUE);
        return this;
    }

    public FileObjectCopier setSource(FileObject source) {
        this.source = source;
        refs.putIfAbsent(this.source, Boolean.FALSE);
        return this;
    }

    public FileObjectCopier setTarget(String target) {
        this.target = resolve(target);
        refs.putIfAbsent(this.target, Boolean.FALSE);
        return this;
    }

    public FileObjectCopier setTarget(FileObject target) {
        this.target = target;
        refs.putIfAbsent(this.target, Boolean.FALSE);
        return this;
    }

    public FileObjectCopier withSource(Function<FileObject, FileObject> function) {
        this.source = function.apply(source);
        refs.putIfAbsent(this.source, Boolean.TRUE);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <OP extends FileOperation> FileObjectCopier withSourceOperation(Class<OP> operation,
                                                                           BiFunction<FileObject, OP, FileObject> function) {
        try {
            this.source = function.apply(source, (OP) source.getFileOperations().getOperation(operation));
            refs.putIfAbsent(this.source, Boolean.TRUE);
            return this;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileObjectCopier withTarget(BiFunction<FileObject, FileObject, FileObject> function) {
        this.target = function.apply(source, target);
        refs.putIfAbsent(this.target, Boolean.TRUE);
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
        for (Map.Entry<FileObject, Boolean> e : refs.entrySet()) {
            try {
                if (e.getValue()) {
                    e.getKey().close();
                }
            } catch (Exception ignore) {
            }
        }
        refs.clear();
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
