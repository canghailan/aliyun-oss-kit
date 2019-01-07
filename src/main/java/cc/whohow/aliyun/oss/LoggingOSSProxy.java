package cc.whohow.aliyun.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 阿里云OSS调试代理
 */
public class LoggingOSSProxy implements OSS {
    private final Pattern PLACEHOLDER = Pattern.compile("\\{}");
    private final Log log = LogFactory.getLog(OSS.class);
    private final OSS oss;

    public LoggingOSSProxy(OSS oss) {
        this.oss = oss;
    }

    private void debug(String format, Object... args) {
        if (log.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer(format.length() + args.length * 16);
            Matcher matcher = PLACEHOLDER.matcher(format);
            for (Object arg : args) {
                if (matcher.find()) {
                    matcher.appendReplacement(buffer, Matcher.quoteReplacement(Objects.toString(arg)));
                } else {
                    break;
                }
            }
            matcher.appendTail(buffer);
            log.debug(buffer);
        }
    }

    @Override
    public void switchCredentials(Credentials creds) {
        debug("switchCredentials: {} {}", creds.getAccessKeyId(), creds.getSecretAccessKey());
        oss.switchCredentials(creds);
    }

    @Override
    public void shutdown() {
        debug("shutdown: {}", oss);
        oss.shutdown();
    }

    @Override
    public Bucket createBucket(String bucketName) throws OSSException, ClientException {
        debug("createBucket: {}", bucketName);
        return oss.createBucket(bucketName);
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest) throws OSSException, ClientException {
        debug("createBucket: {}", createBucketRequest.getBucketName());
        return oss.createBucket(createBucketRequest);
    }

    @Override
    public void deleteBucket(String bucketName) throws OSSException, ClientException {
        debug("deleteBucket: {}", bucketName);
        oss.deleteBucket(bucketName);
    }

    @Override
    public void deleteBucket(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucket: {}", genericRequest.getBucketName());
        oss.deleteBucket(genericRequest);
    }

    @Override
    public List<Bucket> listBuckets() throws OSSException, ClientException {
        debug("listBuckets");
        return oss.listBuckets();
    }

    @Override
    public BucketList listBuckets(String prefix, String marker, Integer maxKeys) throws OSSException, ClientException {
        debug("listBuckets: {} {} {}", prefix, marker, maxKeys);
        return oss.listBuckets(prefix, marker, maxKeys);
    }

    @Override
    public BucketList listBuckets(ListBucketsRequest listBucketsRequest) throws OSSException, ClientException {
        debug("listBuckets: {} {} {}", listBucketsRequest.getPrefix(), listBucketsRequest.getMarker(), listBucketsRequest.getPrefix());
        return oss.listBuckets(listBucketsRequest);
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws OSSException, ClientException {
        debug("setBucketAcl: {}", bucketName);
        oss.setBucketAcl(bucketName, acl);
    }

    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws OSSException, ClientException {
        debug("setBucketAcl: {}", setBucketAclRequest.getBucketName());
        oss.setBucketAcl(setBucketAclRequest);
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName) throws OSSException, ClientException {
        debug("getBucketAcl: {}", bucketName);
        return oss.getBucketAcl(bucketName);
    }

    @Override
    public AccessControlList getBucketAcl(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketAcl: {}", genericRequest.getBucketName());
        return oss.getBucketAcl(genericRequest);
    }

    @Override
    public BucketMetadata getBucketMetadata(String bucketName) throws OSSException, ClientException {
        debug("getBucketMetadata: {}", bucketName);
        return oss.getBucketMetadata(bucketName);
    }

    @Override
    public BucketMetadata getBucketMetadata(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketMetadata: {}", genericRequest.getBucketName());
        return oss.getBucketMetadata(genericRequest);
    }

    @Override
    public void setBucketReferer(String bucketName, BucketReferer referer) throws OSSException, ClientException {
        debug("setBucketReferer: {} {}", bucketName, referer.getRefererList());
        oss.setBucketReferer(bucketName, referer);
    }

    @Override
    public void setBucketReferer(SetBucketRefererRequest setBucketRefererRequest) throws OSSException, ClientException {
        debug("setBucketReferer: {} {}", setBucketRefererRequest.getBucketName(), setBucketRefererRequest.getReferer().getRefererList());
        oss.setBucketReferer(setBucketRefererRequest);
    }

    @Override
    public BucketReferer getBucketReferer(String bucketName) throws OSSException, ClientException {
        debug("getBucketReferer: {}", bucketName);
        return oss.getBucketReferer(bucketName);
    }

    @Override
    public BucketReferer getBucketReferer(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketReferer: {}", genericRequest.getBucketName());
        return oss.getBucketReferer(genericRequest);
    }

    @Override
    public String getBucketLocation(String bucketName) throws OSSException, ClientException {
        debug("getBucketLocation: {}", bucketName);
        return oss.getBucketLocation(bucketName);
    }

    @Override
    public String getBucketLocation(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketLocation: {}", genericRequest.getBucketName());
        return oss.getBucketLocation(genericRequest);
    }

    @Override
    public void setBucketTagging(String bucketName, Map<String, String> tags) throws OSSException, ClientException {
        debug("setBucketTagging: {} {}", bucketName, tags);
        oss.setBucketTagging(bucketName, tags);
    }

    @Override
    public void setBucketTagging(String bucketName, TagSet tagSet) throws OSSException, ClientException {
        debug("setBucketTagging: {} {}", bucketName, tagSet.getAllTags());
        oss.setBucketTagging(bucketName, tagSet);
    }

    @Override
    public void setBucketTagging(SetBucketTaggingRequest setBucketTaggingRequest) throws OSSException, ClientException {
        debug("setBucketTagging: {} {}", setBucketTaggingRequest.getBucketName(), setBucketTaggingRequest.getTagSet().getAllTags());
        oss.setBucketTagging(setBucketTaggingRequest);
    }

    @Override
    public TagSet getBucketTagging(String bucketName) throws OSSException, ClientException {
        debug("getBucketTagging: {}", bucketName);
        return oss.getBucketTagging(bucketName);
    }

    @Override
    public TagSet getBucketTagging(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketTagging: {}", genericRequest.getBucketName());
        return oss.getBucketTagging(genericRequest);
    }

    @Override
    public void deleteBucketTagging(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketTagging: {}", bucketName);
        oss.deleteBucketTagging(bucketName);
    }

    @Override
    public void deleteBucketTagging(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketTagging: {}", genericRequest.getBucketName());
        oss.deleteBucketTagging(genericRequest);
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws OSSException, ClientException {
        debug("doesBucketExist: {}", bucketName);
        return oss.doesBucketExist(bucketName);
    }

    @Override
    public boolean doesBucketExist(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("doesBucketExist: {}", genericRequest.getBucketName());
        return oss.doesBucketExist(genericRequest);
    }

    @Override
    public ObjectListing listObjects(String bucketName) throws OSSException, ClientException {
        debug("listObjects: {}", bucketName);
        return oss.listObjects(bucketName);
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix) throws OSSException, ClientException {
        debug("listObjects: {} {}", bucketName, prefix);
        return oss.listObjects(bucketName, prefix);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException {
        debug("listObjects: {} {} {} {}", listObjectsRequest.getBucketName(), listObjectsRequest.getPrefix(), listObjectsRequest.getMarker(), listObjectsRequest.getMaxKeys());
        return oss.listObjects(listObjectsRequest);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input) throws OSSException, ClientException {
        debug("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, input);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException, ClientException {
        debug("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, input, metadata);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file, ObjectMetadata metadata) throws OSSException, ClientException {
        debug("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, file, metadata);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file) throws OSSException, ClientException {
        debug("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, file);
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws OSSException, ClientException {
        debug("putObject: {} {}", putObjectRequest.getBucketName(), putObjectRequest.getKey());
        return oss.putObject(putObjectRequest);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders) throws OSSException, ClientException {
        debug("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, filePath, requestHeaders);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders, boolean useChunkEncoding) throws OSSException, ClientException {
        debug("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, filePath, requestHeaders, useChunkEncoding);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength, Map<String, String> requestHeaders) throws OSSException, ClientException {
        debug("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, requestContent, contentLength, requestHeaders);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength, Map<String, String> requestHeaders, boolean useChunkEncoding) throws OSSException, ClientException {
        debug("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, requestContent, contentLength, requestHeaders, useChunkEncoding);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws OSSException, ClientException {
        debug("copyObject: {} {} -> {} {}", sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        return oss.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws OSSException, ClientException {
        debug("copyObject: {} {} -> {} {}",
                copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey(),
                copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
        return oss.copyObject(copyObjectRequest);
    }

    @Override
    public OSSObject getObject(String bucketName, String key) throws OSSException, ClientException {
        debug("getObject: {} {}", bucketName, key);
        return oss.getObject(bucketName, key);
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File file) throws OSSException, ClientException {
        debug("getObject: {} {}", getObjectRequest.getBucketName(), getObjectRequest.getKey());
        return oss.getObject(getObjectRequest, file);
    }

    @Override
    public OSSObject getObject(GetObjectRequest getObjectRequest) throws OSSException, ClientException {
        debug("getObject: {} {}", getObjectRequest.getBucketName(), getObjectRequest.getKey());
        return oss.getObject(getObjectRequest);
    }

    @Override
    public OSSObject selectObject(SelectObjectRequest selectObjectRequest) throws OSSException, ClientException {
        debug("selectObject: {} {} {}", selectObjectRequest.getBucketName(), selectObjectRequest.getKey(), selectObjectRequest.getExpression());
        return oss.selectObject(selectObjectRequest);
    }

    @Override
    public OSSObject getObject(URL signedUrl, Map<String, String> requestHeaders) throws OSSException, ClientException {
        debug("getObject: {}", signedUrl);
        return oss.getObject(signedUrl, requestHeaders);
    }

    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(String bucketName, String key) throws OSSException, ClientException {
        debug("getSimplifiedObjectMeta: {} {}", bucketName, key);
        return oss.getSimplifiedObjectMeta(bucketName, key);
    }

    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getSimplifiedObjectMeta: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getSimplifiedObjectMeta(genericRequest);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key) throws OSSException, ClientException {
        debug("getObjectMetadata: {} {}", bucketName, key);
        return oss.getObjectMetadata(bucketName, key);
    }

    @Override
    public ObjectMetadata getObjectMetadata(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getObjectMetadata: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getObjectMetadata(genericRequest);
    }

    @Override
    public SelectObjectMetadata createSelectObjectMetadata(CreateSelectObjectMetadataRequest createSelectObjectMetadataRequest) throws OSSException, ClientException {
        return oss.createSelectObjectMetadata(createSelectObjectMetadataRequest);
    }

    @Override
    public AppendObjectResult appendObject(AppendObjectRequest appendObjectRequest) throws OSSException, ClientException {
        debug("appendObject: {} {} {}", appendObjectRequest.getBucketName(), appendObjectRequest.getKey(), appendObjectRequest.getPosition());
        return oss.appendObject(appendObjectRequest);
    }

    @Override
    public void deleteObject(String bucketName, String key) throws OSSException, ClientException {
        debug("deleteObject: {} {}", bucketName, key);
        oss.deleteObject(bucketName, key);
    }

    @Override
    public void deleteObject(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteObject: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        oss.deleteObject(genericRequest);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException {
        debug("deleteObjects: {} {}", deleteObjectsRequest.getBucketName(), deleteObjectsRequest.getKeys());
        return oss.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException {
        debug("doesObjectExist: {} {}", bucketName, key);
        return oss.doesObjectExist(bucketName, key);
    }

    @Override
    public boolean doesObjectExist(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("doesObjectExist: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.doesObjectExist(genericRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key, boolean isOnlyInOSS) {
        debug("doesObjectExist: {} {}", bucketName, key);
        return oss.doesObjectExist(bucketName, key, isOnlyInOSS);
    }

    @Override
    @Deprecated
    public boolean doesObjectExist(HeadObjectRequest headObjectRequest) throws OSSException, ClientException {
        debug("doesObjectExist: {} {}", headObjectRequest.getBucketName(), headObjectRequest.getKey());
        return oss.doesObjectExist(headObjectRequest);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList cannedAcl) throws OSSException, ClientException {
        debug("setObjectAcl: {} {}", bucketName, key);
        oss.setObjectAcl(bucketName, key, cannedAcl);
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest) throws OSSException, ClientException {
        debug("setObjectAcl: {} {}", setObjectAclRequest.getBucketName(), setObjectAclRequest.getKey());
        oss.setObjectAcl(setObjectAclRequest);
    }

    @Override
    public ObjectAcl getObjectAcl(String bucketName, String key) throws OSSException, ClientException {
        debug("getObjectAcl: {} {}", bucketName, key);
        return oss.getObjectAcl(bucketName, key);
    }

    @Override
    public ObjectAcl getObjectAcl(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getObjectAcl: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getObjectAcl(genericRequest);
    }

    @Override
    public RestoreObjectResult restoreObject(String bucketName, String key) throws OSSException, ClientException {
        debug("restoreObject: {} {}", bucketName, key);
        return oss.restoreObject(bucketName, key);
    }

    @Override
    public RestoreObjectResult restoreObject(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("restoreObject: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.restoreObject(genericRequest);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws ClientException {
        debug("generatePresignedUrl: {} {}", bucketName, key);
        return oss.generatePresignedUrl(bucketName, key, expiration);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws ClientException {
        debug("generatePresignedUrl: {} {}", bucketName, key);
        return oss.generatePresignedUrl(bucketName, key, expiration, method);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest request) throws ClientException {
        debug("generatePresignedUrl: {} {}", request.getBucketName(), request.getKey());
        return oss.generatePresignedUrl(request);
    }

    @Override
    public void putBucketImage(PutBucketImageRequest request) throws OSSException, ClientException {
        debug("putBucketImage: {}", request.getBucketName());
        oss.putBucketImage(request);
    }

    @Override
    public GetBucketImageResult getBucketImage(String bucketName) throws OSSException, ClientException {
        debug("getBucketImage: {}", bucketName);
        return oss.getBucketImage(bucketName);
    }

    @Override
    public GetBucketImageResult getBucketImage(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketImage: {}", bucketName);
        return oss.getBucketImage(bucketName, genericRequest);
    }

    @Override
    public void deleteBucketImage(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketImage: {}", bucketName);
        oss.deleteBucketImage(bucketName);
    }

    @Override
    public void deleteBucketImage(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketImage: {}", bucketName);
        oss.deleteBucketImage(bucketName, genericRequest);
    }

    @Override
    public void deleteImageStyle(String bucketName, String styleName) throws OSSException, ClientException {
        debug("deleteImageStyle: {} {}", bucketName, styleName);
        oss.deleteImageStyle(bucketName, styleName);
    }

    @Override
    public void deleteImageStyle(String bucketName, String styleName, GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteImageStyle: {} {}", bucketName, styleName);
        oss.deleteImageStyle(bucketName, styleName, genericRequest);
    }

    @Override
    public void putImageStyle(PutImageStyleRequest putImageStyleRequest) throws OSSException, ClientException {
        debug("putImageStyle: {} {}", putImageStyleRequest.GetBucketName(), putImageStyleRequest.GetStyle());
        oss.putImageStyle(putImageStyleRequest);
    }

    @Override
    public GetImageStyleResult getImageStyle(String bucketName, String styleName) throws OSSException, ClientException {
        debug("getImageStyle: {} {}", bucketName, styleName);
        return oss.getImageStyle(bucketName, styleName);
    }

    @Override
    public GetImageStyleResult getImageStyle(String bucketName, String styleName, GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getImageStyle: {} {}", bucketName, styleName);
        return oss.getImageStyle(bucketName, styleName, genericRequest);
    }

    @Override
    public List<Style> listImageStyle(String bucketName) throws OSSException, ClientException {
        debug("listImageStyle: {}", bucketName);
        return oss.listImageStyle(bucketName);
    }

    @Override
    public List<Style> listImageStyle(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        debug("listImageStyle: {}", bucketName);
        return oss.listImageStyle(bucketName, genericRequest);
    }

    @Override
    public void setBucketProcess(SetBucketProcessRequest setBucketProcessRequest) throws OSSException, ClientException {
        debug("setBucketProcess: {}", setBucketProcessRequest.getBucketName());
        oss.setBucketProcess(setBucketProcessRequest);
    }

    @Override
    public BucketProcess getBucketProcess(String bucketName) throws OSSException, ClientException {
        debug("getBucketProcess: {}", bucketName);
        return oss.getBucketProcess(bucketName);
    }

    @Override
    public BucketProcess getBucketProcess(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketProcess: {}", genericRequest.getBucketName());
        return oss.getBucketProcess(genericRequest);
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws OSSException, ClientException {
        debug("initiateMultipartUpload: {} {}", request.getBucketName(), request.getKey());
        return oss.initiateMultipartUpload(request);
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws OSSException, ClientException {
        debug("listMultipartUploads: {} {}", request.getBucketName(), request.getKey());
        return oss.listMultipartUploads(request);
    }

    @Override
    public PartListing listParts(ListPartsRequest request) throws OSSException, ClientException {
        debug("listParts: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.listParts(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws OSSException, ClientException {
        debug("uploadPart: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.uploadPart(request);
    }

    @Override
    public UploadPartCopyResult uploadPartCopy(UploadPartCopyRequest request) throws OSSException, ClientException {
        debug("uploadPartCopy: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.uploadPartCopy(request);
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws OSSException, ClientException {
        debug("abortMultipartUpload: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        oss.abortMultipartUpload(request);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws OSSException, ClientException {
        debug("completeMultipartUpload: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.completeMultipartUpload(request);
    }

    @Override
    public void setBucketCORS(SetBucketCORSRequest request) throws OSSException, ClientException {
        debug("setBucketCORS: {}", request.getBucketName());
        oss.setBucketCORS(request);
    }

    @Override
    public List<SetBucketCORSRequest.CORSRule> getBucketCORSRules(String bucketName) throws OSSException, ClientException {
        debug("getBucketCORSRules: {}", bucketName);
        return oss.getBucketCORSRules(bucketName);
    }

    @Override
    public List<SetBucketCORSRequest.CORSRule> getBucketCORSRules(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketCORSRules: {}", genericRequest.getBucketName());
        return oss.getBucketCORSRules(genericRequest);
    }

    @Override
    public void deleteBucketCORSRules(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketCORSRules: {}", bucketName);
        oss.deleteBucketCORSRules(bucketName);
    }

    @Override
    public void deleteBucketCORSRules(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketCORSRules: {}", genericRequest.getBucketName());
        oss.deleteBucketCORSRules(genericRequest);
    }

    @Override
    @Deprecated
    public ResponseMessage optionsObject(OptionsRequest request) throws OSSException, ClientException {
        debug("optionsObject: {} {}", request.getBucketName(), request.getKey());
        return oss.optionsObject(request);
    }

    @Override
    public void setBucketLogging(SetBucketLoggingRequest request) throws OSSException, ClientException {
        debug("setBucketLogging: {}", request.getBucketName());
        oss.setBucketLogging(request);
    }

    @Override
    public BucketLoggingResult getBucketLogging(String bucketName) throws OSSException, ClientException {
        debug("getBucketLogging: {}", bucketName);
        return oss.getBucketLogging(bucketName);
    }

    @Override
    public BucketLoggingResult getBucketLogging(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketLogging: {}", genericRequest.getBucketName());
        return oss.getBucketLogging(genericRequest);
    }

    @Override
    public void deleteBucketLogging(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketLogging: {}", bucketName);
        oss.deleteBucketLogging(bucketName);
    }

    @Override
    public void deleteBucketLogging(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketLogging: {}", genericRequest.getBucketName());
        oss.deleteBucketLogging(genericRequest);
    }

    @Override
    public void setBucketWebsite(SetBucketWebsiteRequest setBucketWebSiteRequest) throws OSSException, ClientException {
        debug("setBucketWebsite: {} {}", setBucketWebSiteRequest.getBucketName(), setBucketWebSiteRequest.getIndexDocument());
        oss.setBucketWebsite(setBucketWebSiteRequest);
    }

    @Override
    public BucketWebsiteResult getBucketWebsite(String bucketName) throws OSSException, ClientException {
        debug("getBucketWebsite: {}", bucketName);
        return oss.getBucketWebsite(bucketName);
    }

    @Override
    public BucketWebsiteResult getBucketWebsite(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketWebsite: {}", genericRequest.getBucketName());
        return oss.getBucketWebsite(genericRequest);
    }

    @Override
    public void deleteBucketWebsite(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketWebsite: {}", bucketName);
        oss.deleteBucketWebsite(bucketName);
    }

    @Override
    public void deleteBucketWebsite(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketWebsite: {}", genericRequest.getBucketName());
        oss.deleteBucketWebsite(genericRequest);
    }

    @Override
    public String generatePostPolicy(Date expiration, PolicyConditions conds) throws ClientException {
        return oss.generatePostPolicy(expiration, conds);
    }

    @Override
    public String calculatePostSignature(String postPolicy) {
        return oss.calculatePostSignature(postPolicy);
    }

    @Override
    public void setBucketLifecycle(SetBucketLifecycleRequest setBucketLifecycleRequest) throws OSSException, ClientException {
        debug("setBucketLifecycle: {}", setBucketLifecycleRequest.getBucketName());
        oss.setBucketLifecycle(setBucketLifecycleRequest);
    }

    @Override
    public List<LifecycleRule> getBucketLifecycle(String bucketName) throws OSSException, ClientException {
        debug("getBucketLifecycle: {}", bucketName);
        return oss.getBucketLifecycle(bucketName);
    }

    @Override
    public List<LifecycleRule> getBucketLifecycle(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketLifecycle: {}", genericRequest.getBucketName());
        return oss.getBucketLifecycle(genericRequest);
    }

    @Override
    public void deleteBucketLifecycle(String bucketName) throws OSSException, ClientException {
        debug("deleteBucketLifecycle: {}", bucketName);
        oss.deleteBucketLifecycle(bucketName);
    }

    @Override
    public void deleteBucketLifecycle(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteBucketLifecycle: {}", genericRequest.getBucketName());
        oss.deleteBucketLifecycle(genericRequest);
    }

    @Override
    public void addBucketReplication(AddBucketReplicationRequest addBucketReplicationRequest) throws OSSException, ClientException {
        debug("addBucketReplication: {} {}", addBucketReplicationRequest.getBucketName(), addBucketReplicationRequest.getTargetBucketName());
        oss.addBucketReplication(addBucketReplicationRequest);
    }

    @Override
    public List<ReplicationRule> getBucketReplication(String bucketName) throws OSSException, ClientException {
        debug("getBucketReplication: {}", bucketName);
        return oss.getBucketReplication(bucketName);
    }

    @Override
    public List<ReplicationRule> getBucketReplication(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketReplication: {}", genericRequest.getBucketName());
        return oss.getBucketReplication(genericRequest);
    }

    @Override
    public void deleteBucketReplication(String bucketName, String replicationRuleID) throws OSSException, ClientException {
        debug("deleteBucketReplication: {}", bucketName);
        oss.deleteBucketReplication(bucketName, replicationRuleID);
    }

    @Override
    public void deleteBucketReplication(DeleteBucketReplicationRequest deleteBucketReplicationRequest) throws OSSException, ClientException {
        debug("deleteBucketReplication: {}", deleteBucketReplicationRequest.getBucketName());
        oss.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(String bucketName, String replicationRuleID) throws OSSException, ClientException {
        debug("getBucketReplicationProgress: {}", bucketName);
        return oss.getBucketReplicationProgress(bucketName, replicationRuleID);
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(GetBucketReplicationProgressRequest getBucketReplicationProgressRequest) throws OSSException, ClientException {
        debug("getBucketReplicationProgress: {}", getBucketReplicationProgressRequest.getBucketName());
        return oss.getBucketReplicationProgress(getBucketReplicationProgressRequest);
    }

    @Override
    public List<String> getBucketReplicationLocation(String bucketName) throws OSSException, ClientException {
        debug("getBucketReplicationLocation: {}", bucketName);
        return oss.getBucketReplicationLocation(bucketName);
    }

    @Override
    public List<String> getBucketReplicationLocation(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketReplicationLocation: {}", genericRequest.getBucketName());
        return oss.getBucketReplicationLocation(genericRequest);
    }

    @Override
    public void addBucketCname(AddBucketCnameRequest addBucketCnameRequest) throws OSSException, ClientException {
        debug("addBucketCname: {} {}", addBucketCnameRequest.getBucketName(), addBucketCnameRequest.getDomain());
        oss.addBucketCname(addBucketCnameRequest);
    }

    @Override
    public List<CnameConfiguration> getBucketCname(String bucketName) throws OSSException, ClientException {
        debug("getBucketCname: {}", bucketName);
        return oss.getBucketCname(bucketName);
    }

    @Override
    public List<CnameConfiguration> getBucketCname(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketCname: {}", genericRequest.getBucketName());
        return oss.getBucketCname(genericRequest);
    }

    @Override
    public void deleteBucketCname(String bucketName, String domain) throws OSSException, ClientException {
        debug("deleteBucketCname: {} {}", bucketName, domain);
        oss.deleteBucketCname(bucketName, domain);
    }

    @Override
    public void deleteBucketCname(DeleteBucketCnameRequest deleteBucketCnameRequest) throws OSSException, ClientException {
        debug("deleteBucketCname: {} {}", deleteBucketCnameRequest.getBucketName(), deleteBucketCnameRequest.getDomain());
        oss.deleteBucketCname(deleteBucketCnameRequest);
    }

    @Override
    public BucketInfo getBucketInfo(String bucketName) throws OSSException, ClientException {
        debug("getBucketInfo: {}", bucketName);
        return oss.getBucketInfo(bucketName);
    }

    @Override
    public BucketInfo getBucketInfo(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketInfo: {}", genericRequest.getBucketName());
        return oss.getBucketInfo(genericRequest);
    }

    @Override
    public BucketStat getBucketStat(String bucketName) throws OSSException, ClientException {
        debug("getBucketStat: {}", bucketName);
        return oss.getBucketStat(bucketName);
    }

    @Override
    public BucketStat getBucketStat(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketStat: {}", genericRequest.getBucketName());
        return oss.getBucketStat(genericRequest);
    }

    @Override
    public void setBucketStorageCapacity(String bucketName, UserQos userQos) throws OSSException, ClientException {
        debug("setBucketStorageCapacity: {} {}", bucketName, userQos.getStorageCapacity());
        oss.setBucketStorageCapacity(bucketName, userQos);
    }

    @Override
    public void setBucketStorageCapacity(SetBucketStorageCapacityRequest setBucketStorageCapacityRequest) throws OSSException, ClientException {
        debug("setBucketStorageCapacity: {} {}", setBucketStorageCapacityRequest.getBucketName(), setBucketStorageCapacityRequest.getUserQos().getStorageCapacity());
        oss.setBucketStorageCapacity(setBucketStorageCapacityRequest);
    }

    @Override
    public UserQos getBucketStorageCapacity(String bucketName) throws OSSException, ClientException {
        debug("getBucketStorageCapacity: {}", bucketName);
        return oss.getBucketStorageCapacity(bucketName);
    }

    @Override
    public UserQos getBucketStorageCapacity(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getBucketStorageCapacity: {}", genericRequest.getBucketName());
        return oss.getBucketStorageCapacity(genericRequest);
    }

    @Override
    public UploadFileResult uploadFile(UploadFileRequest uploadFileRequest) throws Throwable {
        debug("uploadFile: {} {} {}", uploadFileRequest.getBucketName(), uploadFileRequest.getKey(), uploadFileRequest.getUploadFile());
        return oss.uploadFile(uploadFileRequest);
    }

    @Override
    public DownloadFileResult downloadFile(DownloadFileRequest downloadFileRequest) throws Throwable {
        debug("downloadFile: {} {} {}", downloadFileRequest.getBucketName(), downloadFileRequest.getKey(), downloadFileRequest.getDownloadFile());
        return oss.downloadFile(downloadFileRequest);
    }

    @Override
    public CreateLiveChannelResult createLiveChannel(CreateLiveChannelRequest createLiveChannelRequest) throws OSSException, ClientException {
        debug("createLiveChannel: {} {}", createLiveChannelRequest.getBucketName(), createLiveChannelRequest.getLiveChannelName());
        return oss.createLiveChannel(createLiveChannelRequest);
    }

    @Override
    public void setLiveChannelStatus(String bucketName, String liveChannel, LiveChannelStatus status) throws OSSException, ClientException {
        debug("setLiveChannelStatus: {} {} {}", bucketName, liveChannel, status);
        oss.setLiveChannelStatus(bucketName, liveChannel, status);
    }

    @Override
    public void setLiveChannelStatus(SetLiveChannelRequest setLiveChannelRequest) throws OSSException, ClientException {
        debug("setLiveChannelStatus: {} {} {}", setLiveChannelRequest.getBucketName(), setLiveChannelRequest.getLiveChannelName(), setLiveChannelRequest.getLiveChannelStatus());
        oss.setLiveChannelStatus(setLiveChannelRequest);
    }

    @Override
    public LiveChannelInfo getLiveChannelInfo(String bucketName, String liveChannel) throws OSSException, ClientException {
        debug("getLiveChannelInfo: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelInfo(bucketName, liveChannel);
    }

    @Override
    public LiveChannelInfo getLiveChannelInfo(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        debug("getLiveChannelInfo: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelInfo(liveChannelGenericRequest);
    }

    @Override
    public LiveChannelStat getLiveChannelStat(String bucketName, String liveChannel) throws OSSException, ClientException {
        debug("getLiveChannelStat: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelStat(bucketName, liveChannel);
    }

    @Override
    public LiveChannelStat getLiveChannelStat(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        debug("getLiveChannelStat: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelStat(liveChannelGenericRequest);
    }

    @Override
    public void deleteLiveChannel(String bucketName, String liveChannel) throws OSSException, ClientException {
        debug("deleteLiveChannel: {} {}", bucketName, liveChannel);
        oss.deleteLiveChannel(bucketName, liveChannel);
    }

    @Override
    public void deleteLiveChannel(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        debug("deleteLiveChannel: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        oss.deleteLiveChannel(liveChannelGenericRequest);
    }

    @Override
    public List<LiveChannel> listLiveChannels(String bucketName) throws OSSException, ClientException {
        debug("listLiveChannels: {}", bucketName);
        return oss.listLiveChannels(bucketName);
    }

    @Override
    public LiveChannelListing listLiveChannels(ListLiveChannelsRequest listLiveChannelRequest) throws OSSException, ClientException {
        debug("listLiveChannels: {}", listLiveChannelRequest.getBucketName());
        return oss.listLiveChannels(listLiveChannelRequest);
    }

    @Override
    public List<LiveRecord> getLiveChannelHistory(String bucketName, String liveChannel) throws OSSException, ClientException {
        debug("getLiveChannelHistory: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelHistory(bucketName, liveChannel);
    }

    @Override
    public List<LiveRecord> getLiveChannelHistory(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        debug("getLiveChannelHistory: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelHistory(liveChannelGenericRequest);
    }

    @Override
    public void generateVodPlaylist(String bucketName, String liveChannelName, String PlaylistName, long startTime, long endTime) throws OSSException, ClientException {
        debug("generateVodPlaylist: {} {}", bucketName, liveChannelName);
        oss.generateVodPlaylist(bucketName, liveChannelName, PlaylistName, startTime, endTime);
    }

    @Override
    public void generateVodPlaylist(GenerateVodPlaylistRequest generateVodPlaylistRequest) throws OSSException, ClientException {
        debug("generateVodPlaylist: {} {}", generateVodPlaylistRequest.getBucketName(), generateVodPlaylistRequest.getLiveChannelName());
        oss.generateVodPlaylist(generateVodPlaylistRequest);
    }

    @Override
    public String generateRtmpUri(String bucketName, String liveChannelName, String PlaylistName, long expires) throws OSSException, ClientException {
        debug("generateRtmpUri: {} {}", bucketName, liveChannelName);
        return oss.generateRtmpUri(bucketName, liveChannelName, PlaylistName, expires);
    }

    @Override
    public String generateRtmpUri(GenerateRtmpUriRequest generatePushflowUrlRequest) throws OSSException, ClientException {
        debug("generateRtmpUri: {} {}", generatePushflowUrlRequest.getBucketName(), generatePushflowUrlRequest.getLiveChannelName());
        return oss.generateRtmpUri(generatePushflowUrlRequest);
    }

    @Override
    public void createSymlink(String bucketName, String symlink, String target) throws OSSException, ClientException {
        debug("createSymlink: {} {} {}", bucketName, symlink, target);
        oss.createSymlink(bucketName, symlink, target);
    }

    @Override
    public void createSymlink(CreateSymlinkRequest createSymlinkRequest) throws OSSException, ClientException {
        debug("createSymlink: {} {} {}", createSymlinkRequest.getBucketName(), createSymlinkRequest.getSymlink(), createSymlinkRequest.getTarget());
        oss.createSymlink(createSymlinkRequest);
    }

    @Override
    public OSSSymlink getSymlink(String bucketName, String symlink) throws OSSException, ClientException {
        debug("getSymlink: {} {}", bucketName, symlink);
        return oss.getSymlink(bucketName, symlink);
    }

    @Override
    public OSSSymlink getSymlink(GenericRequest genericRequest) throws OSSException, ClientException {
        debug("getSymlink: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getSymlink(genericRequest);
    }

    @Override
    public GenericResult processObject(ProcessObjectRequest processObjectRequest) throws OSSException, ClientException {
        debug("processObjectRequest: {} {} {}", processObjectRequest.getBucketName(), processObjectRequest.getKey(), processObjectRequest.getProcess());
        return oss.processObject(processObjectRequest);
    }

    @Override
    public void createUdf(CreateUdfRequest createUdfRequest) throws OSSException, ClientException {
        debug("createUdf: {}", createUdfRequest.getName());
        oss.createUdf(createUdfRequest);
    }

    @Override
    public UdfInfo getUdfInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("getUdfInfo: {}", genericRequest.getName());
        return oss.getUdfInfo(genericRequest);
    }

    @Override
    public List<UdfInfo> listUdfs() throws OSSException, ClientException {
        debug("listUdfs");
        return oss.listUdfs();
    }

    @Override
    public void deleteUdf(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteUdf: {}", genericRequest.getName());
        oss.deleteUdf(genericRequest);
    }

    @Override
    public void uploadUdfImage(UploadUdfImageRequest uploadUdfImageRequest) throws OSSException, ClientException {
        debug("uploadUdfImage: {}", uploadUdfImageRequest.getName());
        oss.uploadUdfImage(uploadUdfImageRequest);
    }

    @Override
    public List<UdfImageInfo> getUdfImageInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("getUdfImageInfo: {}", genericRequest.getName());
        return oss.getUdfImageInfo(genericRequest);
    }

    @Override
    public void deleteUdfImage(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteUdfImage: {}", genericRequest.getName());
        oss.deleteUdfImage(genericRequest);
    }

    @Override
    public void createUdfApplication(CreateUdfApplicationRequest createUdfApplicationRequest) throws OSSException, ClientException {
        debug("createUdfApplication: {}", createUdfApplicationRequest.getName());
        oss.createUdfApplication(createUdfApplicationRequest);
    }

    @Override
    public UdfApplicationInfo getUdfApplicationInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("getUdfApplicationInfo: {}", genericRequest.getName());
        return oss.getUdfApplicationInfo(genericRequest);
    }

    @Override
    public List<UdfApplicationInfo> listUdfApplications() throws OSSException, ClientException {
        debug("listUdfApplications");
        return oss.listUdfApplications();
    }

    @Override
    public void deleteUdfApplication(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        debug("deleteUdfApplication: {}", genericRequest.getName());
        oss.deleteUdfApplication(genericRequest);
    }

    @Override
    public void upgradeUdfApplication(UpgradeUdfApplicationRequest upgradeUdfApplicationRequest) throws OSSException, ClientException {
        debug("upgradeUdfApplication: {}", upgradeUdfApplicationRequest.getName());
        oss.upgradeUdfApplication(upgradeUdfApplicationRequest);
    }

    @Override
    public void resizeUdfApplication(ResizeUdfApplicationRequest resizeUdfApplicationRequest) throws OSSException, ClientException {
        debug("resizeUdfApplication: {}", resizeUdfApplicationRequest.getName());
        oss.resizeUdfApplication(resizeUdfApplicationRequest);
    }

    @Override
    public UdfApplicationLog getUdfApplicationLog(GetUdfApplicationLogRequest getUdfApplicationLogRequest) throws OSSException, ClientException {
        debug("getUdfApplicationLog: {}", getUdfApplicationLogRequest.getName());
        return oss.getUdfApplicationLog(getUdfApplicationLogRequest);
    }
}
