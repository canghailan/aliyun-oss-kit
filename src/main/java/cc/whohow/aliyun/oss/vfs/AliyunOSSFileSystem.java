package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSContext;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import cc.whohow.aliyun.oss.AliyunOSSUriFactory;
import cc.whohow.vfs.watch.FileWatchMonitor;
import cc.whohow.vfs.watch.FileWatcher;
import com.aliyun.oss.OSS;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractVfsComponent;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

public class AliyunOSSFileSystem extends AbstractVfsComponent implements FileProvider, FileSystem {
    private static final String SCHEME = "oss";
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
    private final FileSystemManager fileSystemManager;
    private final AliyunOSSContext context;
    private final AliyunOSSUriFactory uriFactory;
    private final ScheduledExecutorService executor;
    private final FileWatchMonitor fileWatchMonitor;
    private final CloseableHttpClient httpClient;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    private AliyunOSSFileSystem() {
        this(getVFSManager(),
                AliyunOSS.getContext(),
                AliyunOSS.getUriFactory(),
                AliyunOSS.getExecutor(),
                HttpClientBuilder.create()
                        .setMaxConnPerRoute(1024)
                        .setMaxConnTotal(1024)
                        .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
                        .build());
    }

    public AliyunOSSFileSystem(FileSystemManager fileSystemManager,
                               AliyunOSSContext context,
                               AliyunOSSUriFactory uriFactory,
                               ScheduledExecutorService executor,
                               CloseableHttpClient httpClient) {
        this.fileSystemManager = fileSystemManager;
        this.context = context;
        this.uriFactory = uriFactory;
        this.executor = executor;
        this.fileWatchMonitor = new FileWatchMonitor(executor);
        this.httpClient = httpClient;
    }

    public static AliyunOSSFileSystem getInstance() {
        return SingletonHolder.INSTANCE;
    }

    protected static FileSystemManager getVFSManager() {
        try {
            return VFS.getManager();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public OSS getOSS(AliyunOSSUri uri) {
        return context.getOSS(uri);
    }

    public String getUrl(AliyunOSSUri uri) {
        return uriFactory.getUrl(uri);
    }

    public Collection<String> getCanonicalNames(AliyunOSSUri uri) {
        return uriFactory.getCanonicalUris(uri);
    }

    public String getAccountAlias(AliyunOSSUri uri) {
        return context.getAccountAlias(uri.getBucketName());
    }

    public static String getScheme() {
        return SCHEME;
    }

    @Override
    public FileObject getRoot() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public FileName getRootName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRootURI() {
        throw new UnsupportedOperationException();
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
        return attributes.get(attrName);
    }

    @Override
    public void setAttribute(String attrName, Object value) throws FileSystemException {
        attributes.put(attrName, value);
    }

    @Override
    public FileObject resolveFile(FileName name) throws FileSystemException {
        return null;
    }

    @Override
    public FileObject resolveFile(String name) throws FileSystemException {
        URI uri = URI.create(name);
        if (SCHEME.equals(uri.getScheme())) {
            return new AliyunOSSFileObject(this, new AliyunOSSFileName(uri));
        }
        throw new FileSystemException("");
    }

    @Override
    public synchronized void addListener(FileObject file, FileListener listener) {
        fileWatchMonitor.addListener(new FileWatcher(file, new AliyunOSSFileVersionProvider()), listener);
    }

    @Override
    public synchronized void removeListener(FileObject file, FileListener listener) {
        fileWatchMonitor.removeListener(file, listener);
    }

    @Override
    public void addJunction(String junctionPoint, FileObject targetFile) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void removeJunction(String junctionPoint) throws FileSystemException {
        throw new FileSystemException("");
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
        return fileSystemManager;
    }

    @Override
    public double getLastModTimeAccuracy() {
        return 1000;
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (IOException ignore) {
        } finally {
            super.close();
        }
    }

    @Override
    public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions) throws FileSystemException {
        return resolveFile(uri);
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
        return resolveFile(root).resolveFile(uri).getName();
    }

    private static class SingletonHolder {
        static final AliyunOSSFileSystem INSTANCE = new AliyunOSSFileSystem();
    }
}
