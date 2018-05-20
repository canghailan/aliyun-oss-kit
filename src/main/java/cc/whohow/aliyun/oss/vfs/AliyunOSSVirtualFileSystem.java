package cc.whohow.aliyun.oss.vfs;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperationProvider;
import org.apache.commons.vfs2.operations.FileOperations;
import org.apache.commons.vfs2.provider.*;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AliyunOSSVirtualFileSystem extends AbstractVfsComponent implements FileSystemManager, FileProvider, FileSystem, FileObject {
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(
            Capability.READ_CONTENT,
            Capability.WRITE_CONTENT,
            Capability.APPEND_CONTENT,
            Capability.ATTRIBUTES,
            Capability.LAST_MODIFIED,
            Capability.GET_LAST_MODIFIED,
            Capability.SET_LAST_MODIFIED_FILE,
            Capability.CREATE,
            Capability.DELETE,
            Capability.RENAME,
            Capability.GET_TYPE,
            Capability.LIST_CHILDREN,
            Capability.URI));
    private static final Set<String> SCHEMES = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(
            "oss",
            "http",
            "https"
    )));

    private final NavigableMap<String, FileObject> junctions = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    private final Map<AliyunOSSFileObject, AliyunOSSFileListenerAdapter> listeners = new ConcurrentHashMap<>();
    private final Map<String, List<FileOperationProvider>> operationProviders = new ConcurrentHashMap<>();

    @Override
    public FileObject getBaseFile() throws FileSystemException {
        return this;
    }

    @Override
    public FileObject getRoot() throws FileSystemException {
        return this;
    }

    @Override
    public FileName getRootName() {
        return getName();
    }

    @Override
    public String getRootURI() {
        return getRootName().toString();
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return CAPABILITIES.contains(capability);
    }

    @Override
    public FileObject getParentLayer() throws FileSystemException {
        return null;
    }

    @Override
    public Object getAttribute(String attrName) throws FileSystemException {
        throw new FileSystemException("vfs.provider/get-attribute-not-supported.error");
    }

    @Override
    public void setAttribute(String attrName, Object value) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-attribute-not-supported.error");
    }

    @Override
    public FileObject resolveFile(FileName name) throws FileSystemException {
        return resolveFile(name.toString());
    }

    @Override
    public boolean canRenameTo(FileObject newFile) {
        return false;
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
        throw new FileSystemException("");
    }

    @Override
    public int delete(FileSelector selector) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public int deleteAll() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean exists() throws FileSystemException {
        return true;
    }

    @Override
    public FileObject[] findFiles(FileSelector selector) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void findFiles(FileSelector selector, boolean depthwise, List<FileObject> selected) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject getChild(String name) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject[] getChildren() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileContent getContent() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileOperations getFileOperations() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileSystem getFileSystem() {
        return this;
    }

    @Override
    public FileName getName() {
        return new UriFileName(FileName.SEPARATOR);
    }

    @Override
    public FileObject getParent() throws FileSystemException {
        return null;
    }

    @Override
    public String getPublicURIString() {
        return FileName.SEPARATOR;
    }

    @Override
    public FileType getType() throws FileSystemException {
        return FileType.FOLDER;
    }

    @Override
    public URL getURL() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean isAttached() {
        return true;
    }

    @Override
    public boolean isContentOpen() {
        return false;
    }

    @Override
    public boolean isExecutable() throws FileSystemException {
        return false;
    }

    @Override
    public boolean isFile() throws FileSystemException {
        return false;
    }

    @Override
    public boolean isFolder() throws FileSystemException {
        return true;
    }

    @Override
    public boolean isHidden() throws FileSystemException {
        return false;
    }

    @Override
    public boolean isReadable() throws FileSystemException {
        return false;
    }

    @Override
    public boolean isWriteable() throws FileSystemException {
        return false;
    }

    @Override
    public void moveTo(FileObject destFile) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void refresh() throws FileSystemException {
        // do nothing
    }

    @Override
    public FileObject resolveFile(String name) throws FileSystemException {
        URI uri = URI.create(name);
        if ("oss".equals(uri.getScheme())) {
            return new AliyunOSSFileObject(this, new AliyunOSSFileName(uri));
        }
        if (uri.getQuery() != null || uri.getPath().contains("@")) {
            return new UriFileObject(this, uri);
        }
        for (Map.Entry<String, FileObject> junction : junctions.tailMap(name).entrySet()) {
            if (name.startsWith(junction.getKey())) {
                return junction.getValue().resolveFile(
                        name.substring(junction.getKey().length()));
            }
        }
        return new UriFileObject(this, uri);
    }

    @Override
    public FileObject resolveFile(String name, NameScope scope) throws FileSystemException {
        return resolveFile(name);
    }

    @Override
    public boolean setExecutable(boolean executable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-executable.error", this);
    }

    @Override
    public boolean setReadable(boolean readable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-readable.error", this);
    }

    @Override
    public boolean setWritable(boolean writable, boolean ownerOnly) throws FileSystemException {
        throw new FileSystemException("vfs.provider/set-writeable.error", this);
    }

    @Override
    public synchronized void addListener(FileObject file, FileListener listener) {
        if (file instanceof AliyunOSSFileObject) {
            AliyunOSSFileObject fileObject = (AliyunOSSFileObject) file;
            AliyunOSSFileListenerAdapter listenerAdapter = listeners.putIfAbsent(
                    fileObject, new AliyunOSSFileListenerAdapter(fileObject));
            if (listenerAdapter == null) {
                listenerAdapter = listeners.get(fileObject);
                listenerAdapter.addFileListener(listener);
                listenerAdapter.setScheduledFuture(fileObject.watch(Duration.ofSeconds(1), listenerAdapter));
            } else {
                listenerAdapter.addFileListener(listener);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public synchronized void removeListener(FileObject file, FileListener listener) {
        if (file instanceof AliyunOSSFileObject) {
            AliyunOSSFileObject fileObject = (AliyunOSSFileObject) file;
            AliyunOSSFileListenerAdapter listenerAdapter = listeners.get(fileObject);
            if (listenerAdapter == null) {
                throw new AssertionError();
            }
            listenerAdapter.removeFileListener(listener);
            if (listenerAdapter.getFileListeners().isEmpty()) {
                // 如果没有任何监听，取消监听轮询任务
                listenerAdapter.getScheduledFuture().cancel(true);
                listeners.remove(fileObject);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void addJunction(String junctionPoint, FileObject targetFile) throws FileSystemException {
        junctions.put(junctionPoint, targetFile);
    }

    @Override
    public void removeJunction(String junctionPoint) throws FileSystemException {
        junctions.remove(junctionPoint);
    }

    @Override
    public File replicateFile(FileObject file, FileSelector selector) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileSystemOptions getFileSystemOptions() {
        return null;
    }

    @Override
    public FileSystemManager getFileSystemManager() {
        return this;
    }

    @Override
    public double getLastModTimeAccuracy() {
        return 0;
    }

    @Override
    public FileObject resolveFile(String name, FileSystemOptions fileSystemOptions) throws FileSystemException {
        return resolveFile(name);
    }

    @Override
    public FileObject resolveFile(FileObject baseFile, String name) throws FileSystemException {
        return baseFile.resolveFile(name);
    }

    @Override
    public FileObject resolveFile(File baseFile, String name) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileName resolveName(FileName root, String name) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileName resolveName(FileName root, String name, NameScope scope) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject toFileObject(File file) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject createFileSystem(String provider, FileObject file) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void closeFileSystem(FileSystem filesystem) {
    }

    @Override
    public FileObject createFileSystem(FileObject file) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject createVirtualFileSystem(String rootUri) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject createVirtualFileSystem(FileObject rootFile) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public URLStreamHandlerFactory getURLStreamHandlerFactory() {
        return null;
    }

    @Override
    public boolean canCreateFileSystem(FileObject file) throws FileSystemException {
        return false;
    }

    @Override
    public FilesCache getFilesCache() {
        return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return null;
    }

    @Override
    public Class<?> getFileObjectDecorator() {
        return null;
    }

    @Override
    public Constructor<?> getFileObjectDecoratorConst() {
        return null;
    }

    @Override
    public FileContentInfoFactory getFileContentInfoFactory() {
        return null;
    }

    @Override
    public boolean hasProvider(String scheme) {
        return scheme == null || SCHEMES.contains(scheme);
    }

    @Override
    public String[] getSchemes() {
        return SCHEMES.toArray(new String[0]);
    }

    @Override
    public Collection<Capability> getProviderCapabilities(String scheme) throws FileSystemException {
        return CAPABILITIES;
    }

    @Override
    public FileSystemConfigBuilder getFileSystemConfigBuilder(String scheme) throws FileSystemException {
        return null;
    }

    @Override
    public FileName resolveURI(String uri) throws FileSystemException {
        return resolveFile(uri).getName();
    }

    @Override
    public void addOperationProvider(String scheme, FileOperationProvider operationProvider) throws FileSystemException {
        operationProviders.computeIfAbsent(scheme, self -> new ArrayList<>()).add(operationProvider);
    }

    @Override
    public void addOperationProvider(String[] schemes, FileOperationProvider operationProvider) throws FileSystemException {
        for (String scheme : schemes) {
            addOperationProvider(scheme, operationProvider);
        }
    }

    @Override
    public FileOperationProvider[] getOperationProviders(String scheme) throws FileSystemException {
        return operationProviders.getOrDefault(scheme, Collections.emptyList()).toArray(new FileOperationProvider[0]);
    }

    @Override
    public FileObject resolveFile(URI uri) throws FileSystemException {
        return resolveFile(uri.toString());
    }

    @Override
    public FileObject resolveFile(URL url) throws FileSystemException {
        return resolveFile(url.toString());
    }

    @Override
    public int compareTo(FileObject o) {
        return getName().compareTo(o.getName());
    }

    @Override
    public Iterator<FileObject> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileObject createFileSystem(String scheme, FileObject file, FileSystemOptions fileSystemOptions) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileSystemConfigBuilder getConfigBuilder() {
        return null;
    }

    @Override
    public Collection<Capability> getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public FileName parseUri(FileName root, String uri) throws FileSystemException {
        throw new FileSystemException("");
    }
}
