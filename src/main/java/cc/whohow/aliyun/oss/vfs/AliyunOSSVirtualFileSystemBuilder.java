package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.io.UncheckedIOException;
import java.net.URI;

public class AliyunOSSVirtualFileSystemBuilder {
    private AliyunOSSVirtualFileSystem virtualFileSystem = new AliyunOSSVirtualFileSystem();

    private String accessKeyId;
    private String secretAccessKey;

    public AliyunOSSVirtualFileSystemBuilder setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
        return this;
    }

    public AliyunOSSVirtualFileSystemBuilder setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public AliyunOSSVirtualFileSystemBuilder addBucket(String bucket) {
        return addBucket(new AliyunOSSUri(bucket));
    }

    public AliyunOSSVirtualFileSystemBuilder addBucket(AliyunOSSUri bucket) {
        if (bucket.getAccessKeyId() == null || bucket.getSecretAccessKey() == null) {
            bucket = new AliyunOSSUri(accessKeyId, secretAccessKey,
                    bucket.getBucketName(), bucket.getEndpoint(), bucket.getKey());
        }
        AliyunOSS.configure(bucket);
        return this;
    }

    public AliyunOSSVirtualFileSystemBuilder addCname(String uri, String cname) {
        return addCname(new AliyunOSSUri(uri), URI.create(cname));
    }

    public AliyunOSSVirtualFileSystemBuilder addCname(AliyunOSSUri uri, URI cname) {
        AliyunOSS.configureCname(uri, cname);
        return this;
    }

    public AliyunOSSVirtualFileSystemBuilder addJunction(String junction, String target) {
        AliyunOSSUri aliyunOSSUri = new AliyunOSSUri(target);
        String intranetUrl = AliyunOSS.getIntranetUrl(aliyunOSSUri);
        String extranetUrl = AliyunOSS.getExtranetUrl(aliyunOSSUri);
        String cnameUrl = AliyunOSS.getCnameUrl(aliyunOSSUri);

        FileObject fileObject = resolve(target);
        addJunction(junction, fileObject);
        addJunction(intranetUrl, fileObject);
        addJunction(extranetUrl, fileObject);
        if (cnameUrl != null) {
            addJunction(cnameUrl, fileObject);
        }

        return this;
    }

    public AliyunOSSVirtualFileSystem build() {
        return virtualFileSystem;
    }

    private FileObject resolve(String uri) {
        try {
            return virtualFileSystem.resolveFile(uri);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    private AliyunOSSVirtualFileSystemBuilder addJunction(String junction, FileObject fileObject) {
        try {
            virtualFileSystem.addJunction(junction, fileObject);
            if (junction.startsWith("https://")) {
                virtualFileSystem.addJunction(junction.replaceFirst("^https://", "http://"), fileObject);
            }
            return this;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }
}
