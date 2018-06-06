package cc.whohow.aliyun.oss.vfs;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class UriFileObject implements FileObject {
    private final FileSystem fileSystem;
    private final URI uri;

    public UriFileObject(FileSystem fileSystem, URI uri) {
        this.fileSystem = fileSystem;
        this.uri = uri;
    }

    @Override
    public boolean canRenameTo(FileObject newFile) {
        return false;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void copyFrom(FileObject srcFile, FileSelector selector) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void createFile() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void createFolder() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean delete() throws FileSystemException {
        throw new FileSystemException("vfs.provider/delete-not-supported.error");
    }

    @Override
    public int delete(FileSelector selector) throws FileSystemException {
        throw new FileSystemException("vfs.provider/delete-not-supported.error");
    }

    @Override
    public int deleteAll() throws FileSystemException {
        throw new FileSystemException("vfs.provider/delete-not-supported.error");
    }

    @Override
    public boolean exists() {
        try {
            uri.toURL().openConnection().connect();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public FileObject[] findFiles(FileSelector selector) throws FileSystemException {
        throw new FileSystemException("vfs.provider/find-files.error", this);
    }

    @Override
    public void findFiles(FileSelector selector, boolean depthwise, List<FileObject> selected) throws FileSystemException {
        throw new FileSystemException("vfs.provider/find-files.error", this);
    }

    @Override
    public FileObject getChild(String name) throws FileSystemException {
        throw new FileSystemException("vfs.provider/list-children-not-folder.error", this);
    }

    @Override
    public FileObject[] getChildren() throws FileSystemException {
        throw new FileSystemException("vfs.provider/list-children-not-folder.error", this);
    }

    @Override
    public UriFileContent getContent() {
        return new UriFileContent(this);
    }

    @Override
    public FileOperations getFileOperations() {
        return new UriFileOperations(this);
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public FileName getName() {
        return new UriFileName(uri);
    }

    @Override
    public FileObject getParent() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public String getPublicURIString() {
        return uri.toString();
    }

    @Override
    public FileType getType() {
        return FileType.FILE;
    }

    @Override
    public URL getURL() {
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isAttached() {
        return true;
    }

    @Override
    public boolean isContentOpen() {
        return true;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }

    @Override
    public boolean isFile() {
        return true;
    }

    @Override
    public boolean isFolder() {
        return false;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isReadable() {
        return true;
    }

    @Override
    public boolean isWriteable() {
        return false;
    }

    @Override
    public void moveTo(FileObject destFile) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void refresh() {
        // do nothing
    }

    @Override
    public FileObject resolveFile(String path) throws FileSystemException {
        return getFileSystem().getFileSystemManager().resolveFile(path);
    }

    @Override
    public FileObject resolveFile(String name, NameScope scope) throws FileSystemException {
        if (scope == NameScope.FILE_SYSTEM) {
            return resolveFile(name);
        } else {
            return new UriFileObject(fileSystem, uri.resolve(name));
        }
    }

    @Override
    public boolean setExecutable(boolean executable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean setReadable(boolean readable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean setWritable(boolean writable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public int compareTo(FileObject o) {
        return getName().compareTo(o.getName());
    }

    @Override
    public Iterator<FileObject> iterator() {
        return Collections.<FileObject>singleton(this).iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof UriFileObject) {
            UriFileObject that = (UriFileObject) o;
            return this.uri.equals(that.uri);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public String toString() {
        return uri.toString();
    }
}
