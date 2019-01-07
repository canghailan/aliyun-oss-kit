package cc.whohow.aliyun.oss.vfs;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.Bucket;
import com.aliyuncs.profile.IClientProfile;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractVfsComponent;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AliyunOSSFileSystem extends AbstractVfsComponent implements FileSystem {
    private final AliyunOSSFileProvider fileProvider;
    private final OSS oss;
    private final Bucket bucket;
    private final IClientProfile profile;
    private final String accountAlias;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public AliyunOSSFileSystem(AliyunOSSFileProvider fileProvider,
                               OSS oss,
                               Bucket bucket,
                               IClientProfile profile,
                               String accountAlias) {
        this.fileProvider = fileProvider;
        this.oss = oss;
        this.bucket = bucket;
        this.profile = profile;
        this.accountAlias = accountAlias;
    }

    public AliyunOSSFileProvider getFileProvider() {
        return fileProvider;
    }

    public OSS getOSS() {
        return oss;
    }

    public Bucket getBucket() {
        return bucket;
    }

    public IClientProfile getProfile() {
        return profile;
    }

    public String getAccountAlias() {
        return accountAlias;
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
        return fileProvider.getCapabilities().contains(capability);
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
        if (name instanceof AliyunOSSFileName) {
            return new AliyunOSSFileObject(this, (AliyunOSSFileName) name);
        }
        throw new IllegalArgumentException(name.toString());
    }

    @Override
    public FileObject resolveFile(String name) throws FileSystemException {
        return new AliyunOSSFileObject(this, new AliyunOSSFileName(name));
    }

    @Override
    public synchronized void addListener(FileObject file, FileListener listener) {
        fileProvider.getFileWatchMonitor()
                .addListener(file, listener, new AliyunOSSFileVersionProvider());
    }

    @Override
    public synchronized void removeListener(FileObject file, FileListener listener) {
        fileProvider.getFileWatchMonitor()
                .removeListener(file, listener);
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
        return getContext().getFileSystemManager();
    }

    @Override
    public double getLastModTimeAccuracy() {
        return 1000;
    }
}
