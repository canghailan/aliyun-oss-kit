package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSContext;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import cc.whohow.aliyun.oss.AliyunOSSUrlFactory;
import cc.whohow.vfs.provider.uri.UriFileObject;
import cc.whohow.vfs.watch.FileWatchMonitor;
import cc.whohow.vfs.watch.FileWatcher;
import com.aliyun.oss.OSS;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractVfsComponent;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public class AliyunOSSFileSystem extends AbstractVfsComponent implements FileSystem {
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
    private final AliyunOSSUrlFactory urlFactory;
    private final ScheduledExecutorService executor;
    private final FileWatchMonitor fileWatchMonitor;
    private final CloseableHttpClient httpClient;

    private AliyunOSSFileSystem() {
        this(AliyunOSS.getContext(), AliyunOSS.getUrlFactory(), AliyunOSS.getExecutor());
    }

    public AliyunOSSFileSystem(AliyunOSSContext context,
                               AliyunOSSUrlFactory urlFactory,
                               ScheduledExecutorService executor) {
        this(context, urlFactory, executor,
                HttpClientBuilder.create()
                        .setMaxConnPerRoute(1024)
                        .setMaxConnTotal(1024)
                        .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
                        .build());
    }

    public AliyunOSSFileSystem(AliyunOSSContext context,
                               AliyunOSSUrlFactory urlFactory,
                               ScheduledExecutorService executor,
                               CloseableHttpClient httpClient) {
        try {
            this.fileSystemManager = VFS.getManager();
            this.context = context;
            this.urlFactory = urlFactory;
            this.executor = executor;
            this.fileWatchMonitor = new FileWatchMonitor(executor);
            this.httpClient = httpClient;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static AliyunOSSFileSystem getInstance() {
        return SingletonHolder.INSTANCE;
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
        return urlFactory.getUrl(uri);
    }

    public Collection<String> getUrls(AliyunOSSUri uri) {
        return urlFactory.getUrls(uri);
    }

    @Override
    public FileObject getRoot() throws FileSystemException {
        return null;
    }

    @Override
    public FileName getRootName() {
        return null;
    }

    @Override
    public String getRootURI() {
        return null;
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return false;
    }

    @Override
    public FileObject getParentLayer() throws FileSystemException {
        return null;
    }

    @Override
    public Object getAttribute(String attrName) throws FileSystemException {
        return null;
    }

    @Override
    public void setAttribute(String attrName, Object value) throws FileSystemException {

    }

    @Override
    public FileObject resolveFile(FileName name) throws FileSystemException {
        return null;
    }

    @Override
    public FileObject resolveFile(String name) throws FileSystemException {
        URI uri = URI.create(name);
        if ("oss".equals(uri.getScheme())) {
            return new AliyunOSSFileObject(this, new AliyunOSSFileName(uri));
        }
        if (uri.getQuery() != null || uri.getPath().contains("@")) {
            return new UriFileObject(name);
        }
//        for (Map.Entry<String, FileObject> junction : junctions.tailMap(name).entrySet()) {
//            if (name.startsWith(junction.getKey())) {
//                return junction.getValue().resolveFile(
//                        name.substring(junction.getKey().length()));
//            }
//        }
        return new UriFileObject(name);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeJunction(String junctionPoint) throws FileSystemException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File replicateFile(FileObject file, FileSelector selector) throws FileSystemException {
        return null;
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
        return 0;
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

    private static class SingletonHolder {
        static final AliyunOSSFileSystem INSTANCE = new AliyunOSSFileSystem();
    }
}
