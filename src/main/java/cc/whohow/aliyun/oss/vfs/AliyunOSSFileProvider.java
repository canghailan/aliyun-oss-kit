package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.*;
import cc.whohow.aliyun.oss.vfs.operations.*;
import cc.whohow.vfs.configuration.ConfigurationFile;
import cc.whohow.vfs.watch.FileWatchMonitor;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.Bucket;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.ram.model.v20150501.GetAccountAliasRequest;
import com.aliyuncs.ram.model.v20150501.GetAccountAliasResponse;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperationProvider;
import org.apache.commons.vfs2.provider.AbstractVfsComponent;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AliyunOSSFileProvider extends AbstractVfsComponent implements FileProvider {
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
    protected final Map<String, AliyunOSSFileSystem> fileSystems = new ConcurrentHashMap<>();
    protected ClientConfiguration clientConfiguration;
    protected ScheduledExecutorService executor;
    protected CloseableHttpClient httpClient;
    protected FileWatchMonitor fileWatchMonitor;
    protected AliyunOSSPool ossFactory;
    protected AliyunOSSUriFactory uriFactory;

    @Override
    public void init() throws FileSystemException {
        FileSystemManager fileSystemManager = getContext().getFileSystemManager();
        ConfigurationFile configuration = (ConfigurationFile) fileSystemManager.resolveFile("conf://");
        AliyunOSSProviderConfiguration c = configuration.getProviderConfiguration(AliyunOSSFileProvider.class.getName(), AliyunOSSProviderConfiguration.class);

        clientConfiguration = new ClientConfiguration();
        executor = Executors.newScheduledThreadPool(8);
        httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(1024)
                .setMaxConnTotal(1024)
                .setRedirectStrategy(LaxRedirectStrategy.INSTANCE)
                .build();
        fileWatchMonitor = new FileWatchMonitor(executor);
        ossFactory = new AliyunOSSPool(new LoggingOSSFactory(new AliyunOSSFactory(clientConfiguration)));
        uriFactory = new AliyunOSSUriFactory(this::getBucket);

        for (AliyunOSSProviderConfiguration.Profile profile : c.getProfiles()) {
            createFileSystem(profile);
        }

        if (c.getCnames() != null) {
            for (AliyunOSSProviderConfiguration.Cname cname : c.getCnames()) {
                uriFactory.addCname(new AliyunOSSUri(cname.getUri()), URI.create(cname.getCname()));
            }
        }

        for (FileOperationProvider fileOperationProvider : getFileOperationProviders()) {
            for (String scheme : c.getSchemes()) {
                fileSystemManager.addOperationProvider(scheme, fileOperationProvider);
            }
        }
        String[] http = {"http", "https"};
        fileSystemManager.addOperationProvider(http, new UriGetSignedUrl.Provider());
        fileSystemManager.addOperationProvider(http, new UriProcessImage.Provider());
    }

    public List<FileOperationProvider> getFileOperationProviders() {
        return Arrays.asList(
                new AliyunOSSGetBucket.Provider(),
                new AliyunOSSGetSignedUrl.Provider(),
                new AliyunOSSProcessImage.Provider(),
                new AliyunOSSGetAccountAlias.Provider(),
                new AliyunOSSCompareFileContent.Provider());
    }

    private Bucket getBucket(String name) {
        return fileSystems.get(name).getBucket();
    }

    @Override
    public void close() {
        for (AliyunOSSFileSystem fileSystem : fileSystems.values()) {
            try {
                fileSystem.close();
            } catch (Exception ignore) {
            }
        }
        try {
            executor.shutdownNow();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (Exception ignore) {
        }
        try {
            httpClient.close();
        } catch (Exception ignore) {
        }
        try {
            ossFactory.close();
        } catch (Exception ignore) {
        }
    }

    private void createFileSystem(AliyunOSSProviderConfiguration.Profile profile) throws FileSystemException {
        IClientProfile clientProfile = DefaultProfile.getProfile(null, profile.getAccessKeyId(), profile.getSecretAccessKey());
        CredentialsProvider credentialsProvider = new DefaultCredentialProvider(profile.getAccessKeyId(), profile.getSecretAccessKey());
        OSS oss = new OSSClient(AliyunOSSEndpoints.getDefaultEndpoint(), credentialsProvider, clientConfiguration);
        try {
            String accountAlias = getAccountAlias(clientProfile);
            for (Bucket bucket : oss.listBuckets()) {
                AliyunOSSUri uri = new AliyunOSSUri(
                        profile.getAccessKeyId(),
                        profile.getSecretAccessKey(),
                        bucket.getName(),
                        bucket.getExtranetEndpoint(),
                        null);
                AliyunOSSFileSystem fileSystem = new AliyunOSSFileSystem(
                        this,
                        getOSS(uri),
                        bucket,
                        clientProfile,
                        accountAlias);
                fileSystem.setLogger(getLogger());
                fileSystem.setContext(getContext());
                fileSystem.init();
                fileSystems.put(bucket.getName(), fileSystem);
            }
        } finally {
            oss.shutdown();
        }
    }

    private String getAccountAlias(IClientProfile profile) {
        try {
            IAcsClient acsClient = new DefaultAcsClient(profile);
            GetAccountAliasRequest request = new GetAccountAliasRequest();
            request.setRegionId("cn-hangzhou");
            GetAccountAliasResponse response = acsClient.getAcsResponse(request);
            return response.getAccountAlias();
        } catch (ClientException e) {
            return null;
        }
    }

    @Override
    public AliyunOSSFileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions) throws FileSystemException {
        AliyunOSSFileName name = new AliyunOSSFileName(uri);
        AliyunOSSFileSystem fileSystem = getFileSystem(name);
        if (fileSystem == null) {
            throw new FileSystemException("");
        }
        return new AliyunOSSFileObject(fileSystem, name);
    }

    @Override
    public FileObject createFileSystem(String scheme, FileObject file, FileSystemOptions fileSystemOptions) throws FileSystemException {
        throw new IllegalArgumentException();
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
        return null;
    }

    public AliyunOSSFileSystem getFileSystem(AliyunOSSUri uri) {
        return fileSystems.get(uri.getBucketName());
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public FileWatchMonitor getFileWatchMonitor() {
        return fileWatchMonitor;
    }

    public AliyunOSSUriFactory getUriFactory() {
        return uriFactory;
    }

    public OSS getOSS(AliyunOSSUri uri) {
        if (uri.getAccessKeyId() != null && uri.getSecretAccessKey() != null && uri.getEndpoint() != null) {
            return ossFactory.apply(new AliyunOSSUri(
                    uri.getAccessKeyId(),
                    uri.getSecretAccessKey(),
                    null,
                    AliyunOSSEndpoints.getEndpoint(uri.getEndpoint()),
                    null));
        }
        AliyunOSSFileSystem fileSystem = getFileSystem(uri);
        if (fileSystem != null) {
            return fileSystem.getOSS();
        }
        throw new IllegalArgumentException(uri.toString());
    }
}
