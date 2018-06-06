package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.file.FileTree;
import cc.whohow.aliyun.oss.net.ApacheHttpClient;
import cc.whohow.aliyun.oss.tree.TreePreOrderIterator;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.*;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 阿里云 OSS 对象
 *
 * @see com.aliyun.oss.model.OSSObject
 */
public class AliyunOSSObject {
    protected final OSS oss;
    protected final String bucketName;
    protected final String key;

    public AliyunOSSObject(OSS oss, String bucketName, String key) {
        Objects.requireNonNull(oss);
        Objects.requireNonNull(bucketName);
        Objects.requireNonNull(key);
        this.oss = oss;
        this.bucketName = bucketName;
        this.key = key;
    }

    /**
     * 计算两个URI的相对路径
     */
    public static String relativize(URI ancestor, URI descendant) {
        return ancestor.relativize(descendant).normalize().getPath();
    }

    public OSS getOSS() {
        return oss;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }

    /**
     * 列出文件及文件夹
     */
    public AliyunOSSObjectListingIterator listObjects() {
        return new AliyunOSSObjectListingIterator(oss, bucketName, key, "/");
    }

    /**
     * 递归列出文件及文件夹
     */
    public AliyunOSSObjectListingIterator listObjectsRecursively() {
        return new AliyunOSSObjectListingIterator(oss, bucketName, key);
    }

    /**
     * 列出文件
     */
    public AliyunOSSObjectSummaryIterator listObjectSummaries() {
        return new AliyunOSSObjectSummaryIterator(listObjects());
    }

    /**
     * 递归列出文件
     */
    public AliyunOSSObjectSummaryIterator listObjectSummariesRecursively() {
        return new AliyunOSSObjectSummaryIterator(listObjectsRecursively());
    }

    /**
     * 上传
     */
    public String putObject(InputStream input) {
        return oss.putObject(bucketName, key, input).getETag();
    }

    /**
     * 上传
     */
    public String putObject(InputStream input, ObjectMetadata objectMetadata) {
        return oss.putObject(bucketName, key, input, objectMetadata).getETag();
    }

    /**
     * 上传文件
     */
    public String putObject(File file, ObjectMetadata objectMetadata) {
        return oss.putObject(bucketName, key, file, objectMetadata).getETag();
    }

    /**
     * 上传文件
     */
    public String putObject(File file) {
        return oss.putObject(bucketName, key, file).getETag();
    }

    /**
     * 上传文件夹
     */
    public int putObjectRecursively(File directory) {
        int count = 0;
        URI directoryUri = directory.toURI();
        for (File file : new FileTree(directory, TreePreOrderIterator::new)) {
            if (file.isFile()) {
                String relativePath = relativize(directoryUri, file.toURI());
                String targetKey = key + relativePath;
                oss.putObject(bucketName, targetKey, file);
                count++;
            }
        }
        return count;
    }

    /**
     * 上传链接
     */
    public String putObject(URL url) {
        try {
            HttpClient httpClient = ApacheHttpClient.get();
            HttpResponse response = httpClient.execute(new HttpGet(url.toString()));
            Header contentType = response.getFirstHeader("Content-Type");
            Header contentLength = response.getFirstHeader("Content-Length");

            ObjectMetadata objectMetadata = new ObjectMetadata();
            if (contentType != null) {
                objectMetadata.setContentType(contentType.getValue());
            }
            if (contentLength != null &&
                    contentLength.getValue() != null &&
                    !contentLength.getValue().isEmpty()) {
                objectMetadata.setContentLength(Long.parseLong(contentLength.getValue()));
            }

            try (InputStream stream = response.getEntity().getContent()) {
                return putObject(stream, objectMetadata);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 上传链接
     */
    public String putObject(URL url, ObjectMetadata objectMetadata) {
        try (InputStream stream = url.openStream()) {
            return putObject(stream, objectMetadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 上传（复制） OSS 文件
     */
    public String putObject(AliyunOSSObject object) {
        try (OSSObject ossObject = object.getObject()) {
            return putObject(ossObject.getObjectContent(), ossObject.getObjectMetadata());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 上传（复制） OSS 文件夹
     */
    public int putObjectRecursively(AliyunOSSObject object) {
        int count = 0;
        Iterator<OSSObjectSummary> iterator = object.listObjectSummariesRecursively();
        while (iterator.hasNext()) {
            OSSObjectSummary objectSummary = iterator.next();
            try (OSSObject o = object.getOSS()
                    .getObject(objectSummary.getBucketName(), objectSummary.getKey())) {
                String targetKey = key + o.getKey().substring(object.getKey().length());
                oss.putObject(bucketName, targetKey, o.getObjectContent(), o.getObjectMetadata());
                count++;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return count;
    }

    public boolean isCopyable(AliyunOSSObject source) {
        return source.getOSS().equals(getOSS());
    }

    /**
     * 复制 OSS 文件，要求同一地区
     */
    public String copyFromObject(AliyunOSSObject source) {
        return copyFromObject(source.getBucketName(), source.getKey());
    }

    /**
     * 复制 OSS 文件，要求同一地区
     */
    public String copyFromObject(String sourceBucketName, String sourceKey) {
        return oss.copyObject(sourceBucketName, sourceKey, bucketName, key).getETag();
    }

    /**
     * 复制 OSS 文件夹，要求同一地区
     */
    public int copyFromObjectRecursively(AliyunOSSObject source) {
        return copyFromObjectRecursively(source.getBucketName(), source.getKey());
    }

    /**
     * 复制 OSS 文件夹，要求同一地区
     */
    public int copyFromObjectRecursively(String sourceBucketName, String sourceKey) {
        int count = 0;
        Iterator<OSSObjectSummary> iterator = new AliyunOSSObjectSummaryIterator(
                new AliyunOSSObjectListingIterator(oss, sourceBucketName, sourceKey));
        while (iterator.hasNext()) {
            OSSObjectSummary objectSummary = iterator.next();
            String targetKey = key + objectSummary.getKey().substring(sourceKey.length());
            oss.copyObject(objectSummary.getBucketName(), objectSummary.getKey(), bucketName, targetKey);
            count++;
        }
        return count;
    }

    /**
     * 下载
     */
    public OSSObject getObject() {
        return oss.getObject(bucketName, key);
    }

    /**
     * 下载到指定文件
     */
    public ObjectMetadata getObject(File file) {
        return oss.getObject(new GetObjectRequest(bucketName, key), file);
    }

    /**
     * 下载文件夹
     */
    public int getObjectRecursively(File directory) {
        int count = 0;
        AliyunOSSObjectListingIterator iterator = listObjectsRecursively();
        while (iterator.hasNext()) {
            ObjectListing objectListing = iterator.next();
            for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                File file = new File(directory, objectSummary.getKey().substring(key.length()));
                file.getParentFile().mkdirs();
                oss.getObject(new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey()), file);
                count++;
            }
        }
        return count;
    }

    /**
     * 读取文件内容
     */
    public InputStream getObjectContent() {
        return getObject().getObjectContent();
    }

    /**
     * 读取文件属性
     */
    public SimplifiedObjectMeta getSimplifiedObjectMeta() {
        return oss.getSimplifiedObjectMeta(bucketName, key);
    }

    /**
     * 读取文件属性
     */
    public ObjectMetadata getObjectMetadata() {
        return oss.getObjectMetadata(bucketName, key);
    }

    /**
     * 设置文件属性
     */
    public void setObjectMetadata(ObjectMetadata objectMetadata) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucketName, key, bucketName, key);
        copyObjectRequest.setNewObjectMetadata(objectMetadata);
        oss.copyObject(copyObjectRequest);
    }

    /**
     * 追加上传文件
     */
    public Long appendObject(Long position, File file) {
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, key, file);
        appendObjectRequest.setPosition(position);
        return oss.appendObject(appendObjectRequest).getNextPosition();
    }

    /**
     * 追加上传文件
     */
    public Long appendObject(Long position, File file, ObjectMetadata metadata) {
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, key, file, metadata);
        appendObjectRequest.setPosition(position);
        return oss.appendObject(appendObjectRequest).getNextPosition();
    }

    /**
     * 追加上传
     */
    public Long appendObject(Long position, InputStream input) {
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, key, input);
        appendObjectRequest.setPosition(position);
        return oss.appendObject(appendObjectRequest).getNextPosition();
    }

    /**
     * 追加上传
     */
    public Long appendObject(Long position, InputStream input, ObjectMetadata metadata) {
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, key, input, metadata);
        appendObjectRequest.setPosition(position);
        return oss.appendObject(appendObjectRequest).getNextPosition();
    }

    /**
     * 追加上传流
     */
    public AliyunOSSOutputStream appendObject() {
        return new AliyunOSSOutputStream(oss, bucketName, key);
    }

    /**
     * 追加上传流
     */
    public AliyunOSSOutputStream appendObject(long position) {
        return new AliyunOSSOutputStream(oss, bucketName, key, position);
    }

    /**
     * 删除文件
     */
    public void deleteObject() {
        oss.deleteObject(bucketName, key);
    }

    /**
     * 删除文件夹
     */
    public int deleteObjectsRecursively() {
        int count = 0;
        Iterator<ObjectListing> iterator = listObjectsRecursively();
        while (iterator.hasNext()) {
            ObjectListing objectListing = iterator.next();
            List<OSSObjectSummary> objectSummaries = objectListing.getObjectSummaries();
            if (!objectSummaries.isEmpty()) {
                List<String> keys = objectSummaries.stream()
                        .map(OSSObjectSummary::getKey)
                        .collect(Collectors.toList());
                oss.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys).withQuiet(true));
                count += keys.size();
            }
        }
        return count;
    }

    /**
     * 文件是否存在
     */
    public boolean doesObjectExist() {
        return oss.doesObjectExist(bucketName, key);
    }

    /**
     * 生成签名URL
     */
    public URL generatePresignedUrl(Date expiration) {
        return oss.generatePresignedUrl(bucketName, key, expiration);
    }

    /**
     * 分块上传文件
     */
    public String uploadFile(String file) throws Throwable {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(bucketName, key);
        uploadFileRequest.setUploadFile(file);
        return oss.uploadFile(uploadFileRequest).getMultipartUploadResult().getETag();
    }

    /**
     * 分块上传文件夹
     */
    public int uploadFileRecursively(String directory) throws Throwable {
        int count = 0;
        File dir = new File(directory);
        URI directoryUri = dir.toURI();
        for (File file : new FileTree(dir, TreePreOrderIterator::new)) {
            if (file.isFile()) {
                String relativePath = relativize(directoryUri, file.toURI());
                String targetKey = key + relativePath;
                UploadFileRequest uploadFileRequest = new UploadFileRequest(bucketName, targetKey);
                uploadFileRequest.setUploadFile(file.getAbsolutePath());
                oss.uploadFile(uploadFileRequest);
                count++;
            }
        }
        return count;
    }

    /**
     * 分块下载文件
     */
    public ObjectMetadata downloadFile(String file) throws Throwable {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(bucketName, key);
        downloadFileRequest.setDownloadFile(file);
        return oss.downloadFile(downloadFileRequest).getObjectMetadata();
    }

    /**
     * 分块下载文件夹
     */
    public int downloadFileRecursively(String directory) throws Throwable {
        int count = 0;
        File dir = new File(directory);
        AliyunOSSObjectListingIterator iterator = listObjectsRecursively();
        while (iterator.hasNext()) {
            ObjectListing objectListing = iterator.next();
            for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                File file = new File(dir, objectSummary.getKey().substring(key.length()));
                file.getParentFile().mkdirs();
                DownloadFileRequest downloadFileRequest = new DownloadFileRequest(
                        objectSummary.getBucketName(), objectSummary.getKey());
                downloadFileRequest.setDownloadFile(file.getAbsolutePath());
                oss.downloadFile(downloadFileRequest);
                count++;
            }
        }
        return count;
    }

    /**
     * 读文件内容
     */
    public <T> T read(Function<InputStream, T> deserializer) {
        try (InputStream stream = getObjectContent()) {
            return deserializer.apply(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写文件内容
     */
    public <T> void write(Function<T, byte[]> serializer, T value) {
        write(serializer.apply(value));
    }

    /**
     * 写文件内容
     */
    public <T> void write(BiConsumer<T, OutputStream> serializer, T value) {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            serializer.accept(value, buffer);
            write(buffer.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写文件内容
     */
    public void write(byte[] value) {
        putObject(new ByteArrayInputStream(value));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof AliyunOSSObject) {
            AliyunOSSObject that = (AliyunOSSObject) o;
            return that.bucketName.equals(this.bucketName) &&
                    that.key.equals(this.key);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return bucketName.hashCode() * 31 + key.hashCode();
    }

    @Override
    public String toString() {
        return "oss://" + bucketName + "/" + key;
    }
}
