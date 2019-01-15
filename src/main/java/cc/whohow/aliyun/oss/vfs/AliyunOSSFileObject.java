package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSObject;
import cc.whohow.vfs.*;
import cc.whohow.vfs.operations.ProviderFileOperations;
import cc.whohow.vfs.path.PathParser;
import cc.whohow.vfs.provider.uri.UriFileObject;
import cc.whohow.vfs.selector.FileSelectors;
import cc.whohow.vfs.tree.FileObjectFindTree;
import cc.whohow.vfs.tree.TreeBreadthFirstIterator;
import cc.whohow.vfs.tree.TreePostOrderIterator;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperations;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 阿里云文件对象
 */
public class AliyunOSSFileObject extends AliyunOSSObject
        implements CanonicalNameFileObject, DataFileObject, ListableFileObject, SimpleFileObject, StatelessFileObject {
    protected final AliyunOSSFileSystem fileSystem;
    protected final AliyunOSSFileName name;

    public AliyunOSSFileObject(AliyunOSSFileSystem fileSystem, AliyunOSSFileName name) {
        super(fileSystem.getOSS(), name.getBucketName(), name.getKey());
        if (!Objects.equals(fileSystem.getBucket().getName(), name.getBucketName())) {
            throw new IllegalArgumentException(name.toString());
        }
        this.fileSystem = fileSystem;
        this.name = name;
    }

    /**
     * 拷贝选中文件，支持文件->文件、文件->目录、目录->目录
     */
    public void copyFrom(FileObject srcFile, FileSelector selector) throws FileSystemException {
        if (srcFile.isFile()) {
            if (FileSelectors.include(selector, srcFile)) {
                if (isFile()) {
                    // 拷贝文件
                    copyFile(srcFile);
                } else {
                    // 如果将文件拷贝到目录，自动创建同名文件
                    getChild(srcFile.getName().getBaseName()).copyFile(srcFile);
                }
            }
        } else if (srcFile.isFolder()) {
            if (isFile()) {
                throw new FileSystemException("vfs.provider/copy-file.error", srcFile, this);
            }
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.EXCLUDE_SELF ||
                    selector == Selectors.SELECT_FILES) {
                // 拷贝文件夹
                copyFolder(srcFile);
            } else if (selector == Selectors.SELECT_FOLDERS) {
                // 无需拷贝
                return;
            } else {
                if (srcFile instanceof ListableFileObject) {
                    ListableFileObject listableFileObject = (ListableFileObject) srcFile;
                    if (selector == Selectors.SELECT_CHILDREN ||
                            selector == Selectors.SELECT_SELF_AND_CHILDREN) {
                        // 子节点
                        try (Stream<FileObject> stream = listableFileObject.list()) {
                            copyFileList(srcFile, stream.iterator());
                        }
                    } else {
                        // 查找、拷贝
                        try (Stream<FileObject> stream = listableFileObject.listRecursively(selector)) {
                            copyFileList(srcFile, stream.iterator());
                        }
                    }
                } else {
                    if (selector == Selectors.SELECT_CHILDREN ||
                            selector == Selectors.SELECT_SELF_AND_CHILDREN) {
                        // 子节点
                        copyFileList(srcFile, Arrays.asList(srcFile.getChildren()).iterator());
                    } else {
                        // 查找、拷贝
                        List<FileObject> list = new ArrayList<>();
                        srcFile.findFiles(selector, false, list);
                        copyFileList(srcFile, list.iterator());
                    }
                }
            }
        } else {
            throw new FileSystemException("vfs.provider/copy-file.error", srcFile, this);
        }
    }

    public void copyFile(FileObject fileObject) throws FileSystemException {
//        assert isFile();
//        assert fileObject.isFile();
        if (fileObject instanceof AliyunOSSFileObject) {
            AliyunOSSFileObject f = (AliyunOSSFileObject) fileObject;
            if (isCopyable(f)) {
                copyFromObject(f);
            } else {
                putObject(f);
            }
        } else if (fileObject instanceof UriFileObject) {
            putObject(fileObject.getURL());
        } else {
            try (FileContent fileContent = fileObject.getContent()) {
                try (InputStream stream = fileContent.getInputStream()) {
                    try {
                        FileContentInfo fileContentInfo = fileContent.getContentInfo();
                        ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setContentType(fileContentInfo.getContentType());
                        objectMetadata.setContentEncoding(fileContentInfo.getContentEncoding());
                        putObject(stream, objectMetadata);
                    } catch (Exception ignore) {
                        putObject(stream);
                    }
                } catch (IOException e) {
                    throw new FileSystemException(e);
                }
            }
        }
    }

    public void copyFolder(FileObject fileObject) throws FileSystemException {
//        assert isFolder();
//        assert fileObject.isFolder();
        if (fileObject instanceof AliyunOSSFileObject) {
            AliyunOSSFileObject f = (AliyunOSSFileObject) fileObject;
            if (isCopyable(f)) {
                copyFromObjectRecursively(f);
            } else {
                putObjectRecursively(f);
            }
        } else {
            copyFileList(fileObject, fileObject.iterator());
        }
    }

    public void copyFileList(FileObject fileObject, Iterator<FileObject> iterator) throws FileSystemException {
//        assert isFolder();
//        assert fileObject.isFolder();
        FileName name = fileObject.getName();
        while (iterator.hasNext()) {
            FileObject file = iterator.next();
            if (file.isFile()) {
                String relative = file.getName().getRelativeName(name);
                getRelative(relative).copyFile(file);
            }
        }
    }

    /**
     * 创建文件，无需任何操作
     */
    public void createFile() {
        // do nothing
    }

    /**
     * 创建目录，无需任何操作
     */
    public void createFolder() {
        // do nothing
    }

    @Override
    public boolean delete() throws FileSystemException {
        if (isFile()) {
            if (exists()) {
                deleteObject();
                return true;
            } else {
                return false;
            }
        } else {
            return !find(Selectors.EXCLUDE_SELF).findAny().isPresent();
        }
    }

    /**
     * 删除选中文件
     */
    public int delete(FileSelector selector) throws FileSystemException {
        if (isFile()) {
            // 文件
            if (FileSelectors.include(selector, this)) {
                deleteObject();
                return 1;
            } else {
                return 0;
            }
        }
        // 目录
        if (selector == Selectors.SELECT_ALL ||
                selector == Selectors.EXCLUDE_SELF ||
                selector == Selectors.SELECT_FILES) {
            // 删除所有
            return deleteObjectsRecursively();
        } else {
            // 查找并删除
            return (int) find(selector)
                    .filter(FileObjectFns::isFile)
                    .peek(FileObjectFns::deleteAllQuietly)
                    .count();
        }
    }

    /**
     * 文件是否存在
     */
    public boolean exists() throws FileSystemException {
        if (isFile()) {
            return doesObjectExist();
        } else {
            return true;
        }
    }

    @Override
    public Stream<FileObject> find(FileSelector selector, boolean depthwise) throws FileSystemException {
        if (isFile()) {
            // 文件
            if (FileSelectors.include(selector, this)) {
                return Stream.of(this);
            } else {
                return Stream.empty();
            }
        }
        // 目录
        if (selector == Selectors.SELECT_SELF) {
            return Stream.of(this);
        }
        if (selector == Selectors.SELECT_SELF_AND_CHILDREN) {
            Iterator<FileObject> iterator = AliyunOSSFileObjectIterator.create(this, false);
            return Stream.concat(
                    Stream.of(this),
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false));
        }
        if (selector == Selectors.SELECT_CHILDREN) {
            Iterator<FileObject> iterator = AliyunOSSFileObjectIterator.create(this, false);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        }
        if (depthwise) {
            // 深度优先
            return new FileObjectFindTree(this, selector, TreePostOrderIterator::new).stream()
                    .map(FileSelectInfo::getFile);
        }
        // 仅文件，原生查询优化
        if (selector == Selectors.SELECT_FILES) {
            Iterator<FileObject> iterator = AliyunOSSFileObjectIterator.create(this, true, true, false);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        }
        // 默认广度优先，程序性能更好
        return new FileObjectFindTree(this, selector, TreeBreadthFirstIterator::new).stream()
                .map(FileSelectInfo::getFile);
    }

    @Override
    public void findFiles(FileSelector selector, boolean depthwise, List<FileObject> selected) throws FileSystemException {
        ListableFileObject.super.findFiles(selector, depthwise, selected);
    }

    /**
     * 查找文件
     */
    public FileObject[] findFiles(FileSelector selector) throws FileSystemException {
        return ListableFileObject.super.findFiles(selector);
    }

    /**
     * 获取下级文件
     */
    public AliyunOSSFileObject getChild(String name) throws FileSystemException {
        if (!isFolder()) {
            throw new FileSystemException("vfs.provider/list-children-not-folder.error", this);
        }
        return new AliyunOSSFileObject(getFileSystem(), getName().resolveChild(name));
    }

    /**
     * 获取文件内容
     */
    public AliyunOSSFileContent getContent() {
        return new AliyunOSSFileContent(this);
    }

    public FileOperations getFileOperations() {
        return new ProviderFileOperations(this);
    }

    /**
     * 获取文件系统
     */
    public AliyunOSSFileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * 获取文件名
     */
    public AliyunOSSFileName getName() {
        return name;
    }

    /**
     * 获取上级目录
     */
    public AliyunOSSFileObject getParent() {
        AliyunOSSFileName parent = getName().getParent();
        if (parent == null) {
            return null;
        }
        return new AliyunOSSFileObject(getFileSystem(), parent);
    }

    /**
     * 获取展示地址
     */
    public String getPublicURIString() {
        return fileSystem.getFileProvider().getUriFactory().getUrl(getName());
    }

    /**
     * 获取URL
     */
    public URL getURL() {
        try {
            return new URL(getURL(new Date(System.currentTimeMillis() + Duration.ofDays(1).toMillis())));
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取URL
     */
    public String getURL(Date expiration) {
        String cnameUrl = fileSystem.getFileProvider().getUriFactory()
                .getCnameUrl(name, expiration);
        if (cnameUrl != null) {
            return cnameUrl;
        }

        String extranetUrl = fileSystem.getFileProvider().getUriFactory()
                .getExtranetUrl(name);
        String presignedUrl = oss
                .generatePresignedUrl(bucketName, key, expiration)
                .toString();
        int index = presignedUrl.indexOf('?');
        if (index < 0) {
            return extranetUrl;
        }
        return extranetUrl + presignedUrl.substring(index);
    }

    @Override
    public boolean isReadable() throws FileSystemException {
        return true;
    }

    @Override
    public boolean isWriteable() throws FileSystemException {
        return true;
    }

    public FileObject resolveFile(String name, NameScope scope) throws FileSystemException {
        switch (scope) {
            case CHILD: {
                return getChild(name);
            }
            case DESCENDENT: {
                AliyunOSSFileObject file = getRelative(name);
                if (file.getName().equals(getName())) {
                    throw new FileSystemException("vfs.provider/resolve-file.error", name);
                }
                return file;
            }
            case DESCENDENT_OR_SELF: {
                return getRelative(name);
            }
            case FILE_SYSTEM: {
                if (PathParser.isRelative(name)) {
                    return getRelative(name);
                } else {
                    return getFileSystem().getFileSystemManager().resolveFile(name);
                }
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public boolean setReadable(boolean readable, boolean ownerOnly) throws FileSystemException {
        return readable && !ownerOnly;
    }

    @Override
    public boolean setWritable(boolean writable, boolean ownerOnly) throws FileSystemException {
        return writable && !ownerOnly;
    }

    public AliyunOSSFileObject getRelative(String path) {
        return new AliyunOSSFileObject(getFileSystem(), getName().resolveRelative(path));
    }

    public Iterator<FileObject> iterator() {
        try {
            if (isFolder()) {
                return listRecursively().iterator();
            } else {
                return Collections.emptyIterator();
            }
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String putObject(URL url, ObjectMetadata objectMetadata) {
        return putObject(fileSystem.getFileProvider().getHttpClient(), url, objectMetadata);
    }

    @Override
    public Collection<String> getCanonicalNames() {
        return fileSystem.getFileProvider().getUriFactory().getCanonicalUris(name);
    }
}
