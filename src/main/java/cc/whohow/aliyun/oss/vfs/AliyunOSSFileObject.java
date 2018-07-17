package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSObjectAsync;
import cc.whohow.aliyun.oss.tree.TreePostOrderIterator;
import cc.whohow.aliyun.oss.tree.TreePreOrderIterator;
import cc.whohow.aliyun.oss.vfs.find.FileObjectFindTree;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperations;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 阿里云文件对象
 */
public class AliyunOSSFileObject extends AliyunOSSObjectAsync implements FileObject {
    protected final FileSystem fileSystem;
    protected final AliyunOSSFileName name;

    public AliyunOSSFileObject(FileSystem fileSystem, AliyunOSSFileName name) {
        super(AliyunOSS.getOSS(name), name.getBucketName(), name.getKey(), AliyunOSS.getExecutor());
        this.fileSystem = fileSystem;
        this.name = name;
    }

    /**
     * MoveTo
     */
    public boolean canRenameTo(FileObject newFile) {
        return true;
    }

    public void close() {
        // do nothing
    }

    /**
     * 拷贝选中文件，支持文件->文件、文件->目录、目录->目录
     */
    public void copyFrom(FileObject srcFile, FileSelector selector) throws FileSystemException {
        if (srcFile.isFile()) {
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.SELECT_SELF ||
                    selector == Selectors.SELECT_SELF_AND_CHILDREN ||
                    selector == Selectors.SELECT_FILES ||
                    srcFile.findFiles(selector).length > 0) {
                // 拷贝全部快速方法
                copyAllFrom(srcFile);
            }
            // 未选中，不拷贝
        } else if (isFile()) {
            // 不允许将目录拷贝到文件
            throw new FileSystemException("vfs.provider/copy-file.error", srcFile, this);
        } else {
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.EXCLUDE_SELF ||
                    selector == Selectors.SELECT_FILES) {
                // 拷贝全部快速方法
                copyAllFrom(srcFile);
            } else if (srcFile instanceof AliyunOSSFileObject) {
                AliyunOSSFileObject src = (AliyunOSSFileObject) srcFile;
                if (isCopyable(src)) {
                    // 阿里云OSS原生拷贝
                    for (FileSelectInfo selected : src.findFiles(selector, false)) {
                        AliyunOSSFileObject file = (AliyunOSSFileObject) selected.getFile();
                        if (file.isFile()) {
                            String relative = selected.getFile().getName()
                                    .getRelativeName(selected.getBaseFolder().getName());
                            getDescendant(relative).copyFromObject(file);
                        }
                    }
                } else {
                    // 阿里云OSS模拟拷贝
                    for (FileSelectInfo selected : src.findFiles(selector, false)) {
                        AliyunOSSFileObject file = (AliyunOSSFileObject) selected.getFile();
                        if (file.isFile()) {
                            String relative = selected.getFile().getName()
                                    .getRelativeName(selected.getBaseFolder().getName());
                            getDescendant(relative).putObject(file);
                        }
                    }
                }
            } else {
                // 遍历所有选中文件，逐个拷贝
                FileName srcFileName = srcFile.getName();
                for (FileObject file : srcFile.findFiles(selector)) {
                    if (file.isFile()) {
                        String relative = file.getName().getRelativeName(srcFileName);
                        getDescendant(relative).copyFromFileObject(file);
                    }
                }
            }
        }
    }

    /**
     * 快速拷贝所有文件
     */
    public void copyAllFrom(FileObject srcFile) throws FileSystemException {
        if (srcFile instanceof AliyunOSSFileObject) {
            AliyunOSSFileObject src = (AliyunOSSFileObject) srcFile;
            if (src.isFile()) {
                // 如果将文件拷贝到目录，自动创建同名文件
                AliyunOSSFileObject dst = isFile() ? this : getChild(srcFile.getName().getBaseName());
                dst.copyFromFileObject(src);
            } else {
                if (isFile()) {
                    // 不允许将目录拷贝到文件
                    throw new FileSystemException("vfs.provider/copy-file.error", srcFile, this);
                } else if (isCopyable(src)) {
                    // 阿里云OSS原生拷贝
                    copyFromObjectRecursively(src);
                } else {
                    // 阿里云OSS模拟拷贝
                    putObjectRecursively(src);
                }
            }
        } else {
            if (srcFile.isFile()) {
                // 如果将文件拷贝到目录，自动创建同名文件
                AliyunOSSFileObject dst = isFile() ? this : getChild(srcFile.getName().getBaseName());
                // 拷贝文件流
                dst.copyFromFileObject(srcFile);
            } else {
                if (isFile()) {
                    // 不允许将目录拷贝到文件
                    throw new FileSystemException("vfs.provider/copy-file.error", srcFile, this);
                } else {
                    // 遍历所有文件，逐个拷贝
                    FileName srcFileName = srcFile.getName();
                    for (FileObject file : srcFile) {
                        if (file.isFile()) {
                            String relative = file.getName().getRelativeName(srcFileName);
                            getDescendant(relative).copyFromFileObject(file);
                        }
                    }
                }
            }
        }
    }

    /**
     * 拷贝阿里云文件
     */
    protected void copyFromFileObject(AliyunOSSFileObject srcFile) throws FileSystemException {
        if (isCopyable(srcFile)) {
            // 阿里云OSS原生拷贝
            copyFromObject(srcFile);
        } else {
            // 阿里云OSS模拟拷贝
            putObject(srcFile);
        }
    }

    /**
     * 拷贝链接
     */
    protected void copyFromFileObject(UriFileObject srcFile) throws FileSystemException {
        putObject(srcFile.getURL());
    }

    /**
     * 拷贝一般文件
     */
    protected void copyFromFileObject(FileObject srcFile) throws FileSystemException {
        if (srcFile instanceof AliyunOSSFileObject) {
            copyFromFileObject((AliyunOSSFileObject) srcFile);
        } else if (srcFile instanceof UriFileObject) {
            copyFromFileObject((UriFileObject) srcFile);
        } else {
            try (FileContent fileContent = srcFile.getContent()) {
                try (InputStream stream = fileContent.getInputStream()) {
                    ObjectMetadata objectMetadata = getFileContentInfoAsObjectMetadata(fileContent);
                    if (objectMetadata == null) {
                        putObject(stream);
                    } else {
                        putObject(stream, objectMetadata);
                    }
                } catch (IOException e) {
                    throw new FileSystemException(e);
                }
            }
        }
    }

    /**
     * 获取文件内容信息，并转为ObjectMetadata
     */
    private ObjectMetadata getFileContentInfoAsObjectMetadata(FileContent fileContent) {
        try {
            FileContentInfo fileContentInfo = fileContent.getContentInfo();
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType(fileContentInfo.getContentType());
            objectMetadata.setContentEncoding(fileContentInfo.getContentEncoding());
            return objectMetadata;
        } catch (Exception e) {
            return null;
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

    /**
     * 删除文件，但删除所有下级文件
     */
    public boolean delete() {
        if (isFile()) {
            deleteObject();
            return true;
        } else {
            return false;
        }
    }

    /**
     * 删除选中文件
     */
    public int delete(FileSelector selector) {
        if (isFile()) {
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.SELECT_SELF ||
                    selector == Selectors.SELECT_SELF_AND_CHILDREN ||
                    selector == Selectors.SELECT_FILES ||
                    findFiles(selector).length > 0) {
                // 删除全部快速方法
                return deleteAll();
            } else {
                return 0;
            }
        } else {
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.EXCLUDE_SELF ||
                    selector == Selectors.SELECT_FILES) {
                // 删除全部快速方法
                return deleteAll();
            } else {
                // 查找并删除
                return findFiles(selector, false).stream()
                        .map(selectInfo -> (AliyunOSSFileObject) selectInfo.getFile())
                        .filter(AliyunOSSFileObject::isFile)
                        .map(AliyunOSSFileObject::delete)
                        .mapToInt(deleted -> deleted ? 1 : 0)
                        .sum();
            }
        }
    }

    /**
     * 删除自身及所有下级文件
     */
    public int deleteAll() {
        if (isFile()) {
            deleteObject();
            return 1;
        } else {
            return deleteObjectsRecursively();
        }
    }

    /**
     * 文件是否存在
     */
    public boolean exists() {
        if (isFile()) {
            return doesObjectExist();
        } else {
            return true;
        }
    }

    /**
     * 查找文件
     */
    public FileObject[] findFiles(FileSelector selector) {
        if (selector == Selectors.SELECT_SELF) {
            return new FileObject[]{this};
        }
        if (isFile()) {
            if (selector == Selectors.SELECT_ALL ||
                    selector == Selectors.SELECT_SELF_AND_CHILDREN ||
                    selector == Selectors.SELECT_FILES) {
                return new FileObject[]{this};
            }
        } else {
            if (selector == Selectors.SELECT_ALL) {
                AliyunOSSFileObjectIterator iterator = new AliyunOSSFileObjectIterator(this, true);
                return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                        .toArray(FileObject[]::new);
            } else if (selector == Selectors.SELECT_SELF_AND_CHILDREN) {
                List<FileObject> files = new ArrayList<>();
                files.add(this);
                AliyunOSSFileObjectIterator iterator = new AliyunOSSFileObjectIterator(this, false);
                iterator.forEachRemaining(files::add);
                return files.toArray(new FileObject[0]);
            } else if (selector == Selectors.SELECT_CHILDREN) {
                AliyunOSSFileObjectIterator iterator = new AliyunOSSFileObjectIterator(this, false);
                return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                        .toArray(FileObject[]::new);
            } else if (selector == Selectors.EXCLUDE_SELF) {
                return stream()
                        .toArray(FileObject[]::new);
            } else if (selector == Selectors.SELECT_FILES) {
                return stream()
                        .map(file -> (AliyunOSSFileObject) file)
                        .filter(AliyunOSSFileObject::isFile)
                        .toArray(FileObject[]::new);
            } else if (selector == Selectors.SELECT_FOLDERS) {
                return stream()
                        .map(file -> (AliyunOSSFileObject) file)
                        .filter(AliyunOSSFileObject::isFolder)
                        .toArray(FileObject[]::new);
            }
        }
        return findFiles(selector, false).stream()
                .map(FileSelectInfo::getFile)
                .toArray(FileObject[]::new);
    }

    /**
     * 查找文件
     */
    public FileObjectFindTree findFiles(FileSelector selector, boolean depthwise) {
        return depthwise ?
                new FileObjectFindTree(this, selector, TreePostOrderIterator::new) :
                new FileObjectFindTree(this, selector, TreePreOrderIterator::new);
    }

    /**
     * 查找文件
     */
    public void findFiles(FileSelector selector, boolean depthwise, List<FileObject> selected) {
        findFiles(selector, depthwise).stream().map(FileSelectInfo::getFile).forEach(selected::add);
    }

    /**
     * 获取下级文件
     */
    public AliyunOSSFileObject getChild(String name) throws FileSystemException {
        if (isFile()) {
            throw new FileSystemException("vfs.provider/list-children-not-folder.error", this);
        }
        return new AliyunOSSFileObject(getFileSystem(), getName().resolveChild(name));
    }

    /**
     * 获取下级文件列表
     */
    public FileObject[] getChildren() throws FileSystemException {
        if (isFile()) {
            throw new FileSystemException("vfs.provider/list-children-not-folder.error", this);
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSFileObjectIterator(this, false), 0), false)
                .toArray(FileObject[]::new);
    }

    /**
     * 获取文件内容
     */
    public AliyunOSSFileContent getContent() {
        return new AliyunOSSFileContent(this);
    }

    public FileOperations getFileOperations() {
        return new AliyunOSSFileOperations(this);
    }

    /**
     * 获取文件系统
     */
    public FileSystem getFileSystem() {
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
        return AliyunOSS.getUrl(getName());
    }

    /**
     * 获取文件类型
     */
    public FileType getType() {
        return getName().getType();
    }

    /**
     * 获取URL
     */
    public URL getURL() {
        try {
            return new URL(getPublicURIString());
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isAttached() {
        return true;
    }

    public boolean isContentOpen() {
        return true;
    }

    public boolean isExecutable() {
        return false;
    }

    public boolean isFile() {
        return getType() == FileType.FILE;
    }

    public boolean isFolder() {
        return getType() == FileType.FOLDER;
    }

    public boolean isHidden() {
        return false;
    }

    public boolean isReadable() {
        return true;
    }

    public boolean isWriteable() {
        return true;
    }

    /**
     * 移动文件
     */
    public void moveTo(FileObject destFile) throws FileSystemException {
        destFile.copyFrom(this, Selectors.SELECT_ALL);
        delete();
    }

    public void refresh() {
        // do nothing
    }

    public FileObject resolveFile(String path) throws FileSystemException {
        if (AliyunOSSFileName.isRelative(path)) {
            return getDescendant(path);
        } else {
            return getFileSystem().getFileSystemManager().resolveFile(path);
        }
    }

    public FileObject resolveFile(String name, NameScope scope) throws FileSystemException {
        switch (scope) {
            case CHILD: {
                return getChild(name);
            }
            case DESCENDENT: {
                if (name.isEmpty()) {
                    throw new FileSystemException("vfs.provider/resolve-file.error", name);
                }
                return getDescendant(name);
            }
            case DESCENDENT_OR_SELF: {
                return getDescendant(name);
            }
            case FILE_SYSTEM: {
                return resolveFile(name);
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    public AliyunOSSFileObject getDescendant(String path) {
        return new AliyunOSSFileObject(getFileSystem(), getName().resolveRelative(path));
    }

    public boolean setExecutable(boolean executable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-executable.error", this);
    }

    public boolean setReadable(boolean readable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-readable.error", this);
    }

    public boolean setWritable(boolean writable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-writeable.error", this);
    }

    public int compareTo(FileObject o) {
        return getName().compareTo(o.getName());
    }

    public Iterator<FileObject> iterator() {
        if (isFile()) {
            return Collections.emptyIterator();
        }
        return new AliyunOSSFileObjectIterator(this, true);
    }

    public Stream<FileObject> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
