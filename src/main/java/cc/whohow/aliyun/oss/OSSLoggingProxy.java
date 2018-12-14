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

public class OSSLoggingProxy implements OSS {
    private final Pattern PLACEHOLDER = Pattern.compile("\\{}");
    private final Log log = LogFactory.getLog(OSS.class);
    private final OSS oss;

    public OSSLoggingProxy(OSS oss) {
        this.oss = oss;
    }

    private void trace(String format, Object... args) {
        if (log.isInfoEnabled()) {
            StringBuffer buffer = new StringBuffer(format.length() + args.length * 16);
            Matcher matcher = PLACEHOLDER.matcher(format);
            for (Object arg : args) {
                if (matcher.find()) {
                    matcher.appendReplacement(buffer, Objects.toString(arg));
                } else {
                    break;
                }
            }
            matcher.appendTail(buffer);
            log.info(buffer);
        }
    }

    @Override
    public void switchCredentials(Credentials creds) {
        trace("switchCredentials: {} {}", creds.getAccessKeyId(), creds.getSecretAccessKey());
        oss.switchCredentials(creds);
    }

    @Override
    public void shutdown() {
        trace("shutdown: {}", oss);
        oss.shutdown();
    }

    @Override
    public Bucket createBucket(String bucketName) throws OSSException, ClientException {
        trace("createBucket: {}", bucketName);
        return oss.createBucket(bucketName);
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest) throws OSSException, ClientException {
        trace("createBucket: {}", createBucketRequest.getBucketName());
        return oss.createBucket(createBucketRequest);
    }

    @Override
    public void deleteBucket(String bucketName) throws OSSException, ClientException {
        trace("deleteBucket: {}", bucketName);
        oss.deleteBucket(bucketName);
    }

    @Override
    public void deleteBucket(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucket: {}", genericRequest.getBucketName());
        oss.deleteBucket(genericRequest);
    }

    @Override
    public List<Bucket> listBuckets() throws OSSException, ClientException {
        trace("listBuckets");
        return oss.listBuckets();
    }

    @Override
    public BucketList listBuckets(String prefix, String marker, Integer maxKeys) throws OSSException, ClientException {
        trace("listBuckets: {} {} {}", prefix, marker, maxKeys);
        return oss.listBuckets(prefix, marker, maxKeys);
    }

    @Override
    public BucketList listBuckets(ListBucketsRequest listBucketsRequest) throws OSSException, ClientException {
        trace("listBuckets: {} {} {}", listBucketsRequest.getPrefix(), listBucketsRequest.getMarker(), listBucketsRequest.getPrefix());
        return oss.listBuckets(listBucketsRequest);
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws OSSException, ClientException {
        trace("setBucketAcl: {}", bucketName);
        oss.setBucketAcl(bucketName, acl);
    }

    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws OSSException, ClientException {
        trace("setBucketAcl: {}", setBucketAclRequest.getBucketName());
        oss.setBucketAcl(setBucketAclRequest);
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName) throws OSSException, ClientException {
        trace("getBucketAcl: {}", bucketName);
        return oss.getBucketAcl(bucketName);
    }

    @Override
    public AccessControlList getBucketAcl(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketAcl: {}", genericRequest.getBucketName());
        return oss.getBucketAcl(genericRequest);
    }

    @Override
    public BucketMetadata getBucketMetadata(String bucketName) throws OSSException, ClientException {
        trace("getBucketMetadata: {}", bucketName);
        return oss.getBucketMetadata(bucketName);
    }

    @Override
    public BucketMetadata getBucketMetadata(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketMetadata: {}", genericRequest.getBucketName());
        return oss.getBucketMetadata(genericRequest);
    }

    @Override
    public void setBucketReferer(String bucketName, BucketReferer referer) throws OSSException, ClientException {
        trace("setBucketReferer: {} {}", bucketName, referer.getRefererList());
        oss.setBucketReferer(bucketName, referer);
    }

    @Override
    public void setBucketReferer(SetBucketRefererRequest setBucketRefererRequest) throws OSSException, ClientException {
        trace("setBucketReferer: {} {}", setBucketRefererRequest.getBucketName(), setBucketRefererRequest.getReferer().getRefererList());
        oss.setBucketReferer(setBucketRefererRequest);
    }

    @Override
    public BucketReferer getBucketReferer(String bucketName) throws OSSException, ClientException {
        trace("getBucketReferer: {}", bucketName);
        return oss.getBucketReferer(bucketName);
    }

    @Override
    public BucketReferer getBucketReferer(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketReferer: {}", genericRequest.getBucketName());
        return oss.getBucketReferer(genericRequest);
    }

    @Override
    public String getBucketLocation(String bucketName) throws OSSException, ClientException {
        trace("getBucketLocation: {}", bucketName);
        return oss.getBucketLocation(bucketName);
    }

    @Override
    public String getBucketLocation(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketLocation: {}", genericRequest.getBucketName());
        return oss.getBucketLocation(genericRequest);
    }

    @Override
    public void setBucketTagging(String bucketName, Map<String, String> tags) throws OSSException, ClientException {
        trace("setBucketTagging: {} {}", bucketName, tags);
        oss.setBucketTagging(bucketName, tags);
    }

    @Override
    public void setBucketTagging(String bucketName, TagSet tagSet) throws OSSException, ClientException {
        trace("setBucketTagging: {} {}", bucketName, tagSet.getAllTags());
        oss.setBucketTagging(bucketName, tagSet);
    }

    @Override
    public void setBucketTagging(SetBucketTaggingRequest setBucketTaggingRequest) throws OSSException, ClientException {
        trace("setBucketTagging: {} {}", setBucketTaggingRequest.getBucketName(), setBucketTaggingRequest.getTagSet().getAllTags());
        oss.setBucketTagging(setBucketTaggingRequest);
    }

    @Override
    public TagSet getBucketTagging(String bucketName) throws OSSException, ClientException {
        trace("getBucketTagging: {}", bucketName);
        return oss.getBucketTagging(bucketName);
    }

    @Override
    public TagSet getBucketTagging(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketTagging: {}", genericRequest.getBucketName());
        return oss.getBucketTagging(genericRequest);
    }

    @Override
    public void deleteBucketTagging(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketTagging: {}", bucketName);
        oss.deleteBucketTagging(bucketName);
    }

    @Override
    public void deleteBucketTagging(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketTagging: {}", genericRequest.getBucketName());
        oss.deleteBucketTagging(genericRequest);
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws OSSException, ClientException {
        trace("doesBucketExist: {}", bucketName);
        return oss.doesBucketExist(bucketName);
    }

    @Override
    public boolean doesBucketExist(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("doesBucketExist: {}", genericRequest.getBucketName());
        return oss.doesBucketExist(genericRequest);
    }

    @Override
    public ObjectListing listObjects(String bucketName) throws OSSException, ClientException {
        trace("listObjects: {}", bucketName);
        return oss.listObjects(bucketName);
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix) throws OSSException, ClientException {
        trace("listObjects: {} {}", bucketName, prefix);
        return oss.listObjects(bucketName, prefix);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException {
        trace("listObjects: {} {} {} {}", listObjectsRequest.getBucketName(), listObjectsRequest.getPrefix(), listObjectsRequest.getMarker(), listObjectsRequest.getMaxKeys());
        return oss.listObjects(listObjectsRequest);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input) throws OSSException, ClientException {
        trace("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, input);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException, ClientException {
        trace("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, input, metadata);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file, ObjectMetadata metadata) throws OSSException, ClientException {
        trace("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, file, metadata);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file) throws OSSException, ClientException {
        trace("putObject: {} {}", bucketName, key);
        return oss.putObject(bucketName, key, file);
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws OSSException, ClientException {
        trace("putObject: {} {}", putObjectRequest.getBucketName(), putObjectRequest.getKey());
        return oss.putObject(putObjectRequest);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders) throws OSSException, ClientException {
        trace("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, filePath, requestHeaders);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders, boolean useChunkEncoding) throws OSSException, ClientException {
        trace("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, filePath, requestHeaders, useChunkEncoding);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength, Map<String, String> requestHeaders) throws OSSException, ClientException {
        trace("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, requestContent, contentLength, requestHeaders);
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength, Map<String, String> requestHeaders, boolean useChunkEncoding) throws OSSException, ClientException {
        trace("putObject: {}", signedUrl);
        return oss.putObject(signedUrl, requestContent, contentLength, requestHeaders, useChunkEncoding);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws OSSException, ClientException {
        trace("copyObject: {} {} -> {} {}", sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        return oss.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws OSSException, ClientException {
        trace("copyObject: {} {} -> {} {}",
                copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey(),
                copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
        return oss.copyObject(copyObjectRequest);
    }

    @Override
    public OSSObject getObject(String bucketName, String key) throws OSSException, ClientException {
        trace("getObject: {} {}", bucketName, key);
        return oss.getObject(bucketName, key);
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File file) throws OSSException, ClientException {
        trace("getObject: {} {}", getObjectRequest.getBucketName(), getObjectRequest.getKey());
        return oss.getObject(getObjectRequest, file);
    }

    @Override
    public OSSObject getObject(GetObjectRequest getObjectRequest) throws OSSException, ClientException {
        trace("getObject: {} {}", getObjectRequest.getBucketName(), getObjectRequest.getKey());
        return oss.getObject(getObjectRequest);
    }

    @Override
    public OSSObject selectObject(SelectObjectRequest selectObjectRequest) throws OSSException, ClientException {
        trace("selectObject: {} {} {}", selectObjectRequest.getBucketName(), selectObjectRequest.getKey(), selectObjectRequest.getExpression());
        return oss.selectObject(selectObjectRequest);
    }

    @Override
    public OSSObject getObject(URL signedUrl, Map<String, String> requestHeaders) throws OSSException, ClientException {
        trace("getObject: {}", signedUrl);
        return oss.getObject(signedUrl, requestHeaders);
    }

    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(String bucketName, String key) throws OSSException, ClientException {
        trace("getSimplifiedObjectMeta: {} {}", bucketName, key);
        return oss.getSimplifiedObjectMeta(bucketName, key);
    }

    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getSimplifiedObjectMeta: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getSimplifiedObjectMeta(genericRequest);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key) throws OSSException, ClientException {
        trace("getObjectMetadata: {} {}", bucketName, key);
        return oss.getObjectMetadata(bucketName, key);
    }

    @Override
    public ObjectMetadata getObjectMetadata(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getObjectMetadata: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getObjectMetadata(genericRequest);
    }

    @Override
    public SelectObjectMetadata createSelectObjectMetadata(CreateSelectObjectMetadataRequest createSelectObjectMetadataRequest) throws OSSException, ClientException {
        return oss.createSelectObjectMetadata(createSelectObjectMetadataRequest);
    }

    @Override
    public AppendObjectResult appendObject(AppendObjectRequest appendObjectRequest) throws OSSException, ClientException {
        trace("appendObject: {} {} {}", appendObjectRequest.getBucketName(), appendObjectRequest.getKey(), appendObjectRequest.getPosition());
        return oss.appendObject(appendObjectRequest);
    }

    @Override
    public void deleteObject(String bucketName, String key) throws OSSException, ClientException {
        trace("deleteObject: {} {}", bucketName, key);
        oss.deleteObject(bucketName, key);
    }

    @Override
    public void deleteObject(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteObject: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        oss.deleteObject(genericRequest);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException {
        trace("deleteObjects: {} {}", deleteObjectsRequest.getBucketName(), deleteObjectsRequest.getKeys());
        return oss.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException {
        trace("doesObjectExist: {} {}", bucketName, key);
        return oss.doesObjectExist(bucketName, key);
    }

    @Override
    public boolean doesObjectExist(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("doesObjectExist: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.doesObjectExist(genericRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key, boolean isOnlyInOSS) {
        trace("doesObjectExist: {} {}", bucketName, key);
        return oss.doesObjectExist(bucketName, key, isOnlyInOSS);
    }

    @Override
    @Deprecated
    public boolean doesObjectExist(HeadObjectRequest headObjectRequest) throws OSSException, ClientException {
        trace("doesObjectExist: {} {}", headObjectRequest.getBucketName(), headObjectRequest.getKey());
        return oss.doesObjectExist(headObjectRequest);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList cannedAcl) throws OSSException, ClientException {
        trace("setObjectAcl: {} {}", bucketName, key);
        oss.setObjectAcl(bucketName, key, cannedAcl);
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest) throws OSSException, ClientException {
        trace("setObjectAcl: {} {}", setObjectAclRequest.getBucketName(), setObjectAclRequest.getKey());
        oss.setObjectAcl(setObjectAclRequest);
    }

    @Override
    public ObjectAcl getObjectAcl(String bucketName, String key) throws OSSException, ClientException {
        trace("getObjectAcl: {} {}", bucketName, key);
        return oss.getObjectAcl(bucketName, key);
    }

    @Override
    public ObjectAcl getObjectAcl(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getObjectAcl: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getObjectAcl(genericRequest);
    }

    @Override
    public RestoreObjectResult restoreObject(String bucketName, String key) throws OSSException, ClientException {
        trace("restoreObject: {} {}", bucketName, key);
        return oss.restoreObject(bucketName, key);
    }

    @Override
    public RestoreObjectResult restoreObject(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("restoreObject: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.restoreObject(genericRequest);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws ClientException {
        trace("generatePresignedUrl: {} {}", bucketName, key);
        return oss.generatePresignedUrl(bucketName, key, expiration);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws ClientException {
        trace("generatePresignedUrl: {} {}", bucketName, key);
        return oss.generatePresignedUrl(bucketName, key, expiration, method);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest request) throws ClientException {
        trace("generatePresignedUrl: {} {}", request.getBucketName(), request.getKey());
        return oss.generatePresignedUrl(request);
    }

    @Override
    public void putBucketImage(PutBucketImageRequest request) throws OSSException, ClientException {
        trace("putBucketImage: {}", request.getBucketName());
        oss.putBucketImage(request);
    }

    @Override
    public GetBucketImageResult getBucketImage(String bucketName) throws OSSException, ClientException {
        trace("getBucketImage: {}", bucketName);
        return oss.getBucketImage(bucketName);
    }

    @Override
    public GetBucketImageResult getBucketImage(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketImage: {}", bucketName);
        return oss.getBucketImage(bucketName, genericRequest);
    }

    @Override
    public void deleteBucketImage(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketImage: {}", bucketName);
        oss.deleteBucketImage(bucketName);
    }

    @Override
    public void deleteBucketImage(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketImage: {}", bucketName);
        oss.deleteBucketImage(bucketName, genericRequest);
    }

    @Override
    public void deleteImageStyle(String bucketName, String styleName) throws OSSException, ClientException {
        trace("deleteImageStyle: {} {}", bucketName, styleName);
        oss.deleteImageStyle(bucketName, styleName);
    }

    @Override
    public void deleteImageStyle(String bucketName, String styleName, GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteImageStyle: {} {}", bucketName, styleName);
        oss.deleteImageStyle(bucketName, styleName, genericRequest);
    }

    @Override
    public void putImageStyle(PutImageStyleRequest putImageStyleRequest) throws OSSException, ClientException {
        trace("putImageStyle: {} {}", putImageStyleRequest.GetBucketName(), putImageStyleRequest.GetStyle());
        oss.putImageStyle(putImageStyleRequest);
    }

    @Override
    public GetImageStyleResult getImageStyle(String bucketName, String styleName) throws OSSException, ClientException {
        trace("getImageStyle: {} {}", bucketName, styleName);
        return oss.getImageStyle(bucketName, styleName);
    }

    @Override
    public GetImageStyleResult getImageStyle(String bucketName, String styleName, GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getImageStyle: {} {}", bucketName, styleName);
        return oss.getImageStyle(bucketName, styleName, genericRequest);
    }

    @Override
    public List<Style> listImageStyle(String bucketName) throws OSSException, ClientException {
        trace("listImageStyle: {}", bucketName);
        return oss.listImageStyle(bucketName);
    }

    @Override
    public List<Style> listImageStyle(String bucketName, GenericRequest genericRequest) throws OSSException, ClientException {
        trace("listImageStyle: {}", bucketName);
        return oss.listImageStyle(bucketName, genericRequest);
    }

    @Override
    public void setBucketProcess(SetBucketProcessRequest setBucketProcessRequest) throws OSSException, ClientException {
        trace("setBucketProcess: {}", setBucketProcessRequest.getBucketName());
        oss.setBucketProcess(setBucketProcessRequest);
    }

    @Override
    public BucketProcess getBucketProcess(String bucketName) throws OSSException, ClientException {
        trace("getBucketProcess: {}", bucketName);
        return oss.getBucketProcess(bucketName);
    }

    @Override
    public BucketProcess getBucketProcess(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketProcess: {}", genericRequest.getBucketName());
        return oss.getBucketProcess(genericRequest);
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws OSSException, ClientException {
        trace("initiateMultipartUpload: {} {}", request.getBucketName(), request.getKey());
        return oss.initiateMultipartUpload(request);
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws OSSException, ClientException {
        trace("listMultipartUploads: {} {}", request.getBucketName(), request.getKey());
        return oss.listMultipartUploads(request);
    }

    @Override
    public PartListing listParts(ListPartsRequest request) throws OSSException, ClientException {
        trace("listParts: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.listParts(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws OSSException, ClientException {
        trace("uploadPart: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.uploadPart(request);
    }

    @Override
    public UploadPartCopyResult uploadPartCopy(UploadPartCopyRequest request) throws OSSException, ClientException {
        trace("uploadPartCopy: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.uploadPartCopy(request);
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws OSSException, ClientException {
        trace("abortMultipartUpload: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        oss.abortMultipartUpload(request);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws OSSException, ClientException {
        trace("completeMultipartUpload: {} {} {}", request.getBucketName(), request.getKey(), request.getUploadId());
        return oss.completeMultipartUpload(request);
    }

    @Override
    public void setBucketCORS(SetBucketCORSRequest request) throws OSSException, ClientException {
        trace("setBucketCORS: {}", request.getBucketName());
        oss.setBucketCORS(request);
    }

    @Override
    public List<SetBucketCORSRequest.CORSRule> getBucketCORSRules(String bucketName) throws OSSException, ClientException {
        trace("getBucketCORSRules: {}", bucketName);
        return oss.getBucketCORSRules(bucketName);
    }

    @Override
    public List<SetBucketCORSRequest.CORSRule> getBucketCORSRules(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketCORSRules: {}", genericRequest.getBucketName());
        return oss.getBucketCORSRules(genericRequest);
    }

    @Override
    public void deleteBucketCORSRules(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketCORSRules: {}", bucketName);
        oss.deleteBucketCORSRules(bucketName);
    }

    @Override
    public void deleteBucketCORSRules(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketCORSRules: {}", genericRequest.getBucketName());
        oss.deleteBucketCORSRules(genericRequest);
    }

    @Override
    @Deprecated
    public ResponseMessage optionsObject(OptionsRequest request) throws OSSException, ClientException {
        trace("optionsObject: {} {}", request.getBucketName(), request.getKey());
        return oss.optionsObject(request);
    }

    @Override
    public void setBucketLogging(SetBucketLoggingRequest request) throws OSSException, ClientException {
        trace("setBucketLogging: {}", request.getBucketName());
        oss.setBucketLogging(request);
    }

    @Override
    public BucketLoggingResult getBucketLogging(String bucketName) throws OSSException, ClientException {
        trace("getBucketLogging: {}", bucketName);
        return oss.getBucketLogging(bucketName);
    }

    @Override
    public BucketLoggingResult getBucketLogging(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketLogging: {}", genericRequest.getBucketName());
        return oss.getBucketLogging(genericRequest);
    }

    @Override
    public void deleteBucketLogging(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketLogging: {}", bucketName);
        oss.deleteBucketLogging(bucketName);
    }

    @Override
    public void deleteBucketLogging(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketLogging: {}", genericRequest.getBucketName());
        oss.deleteBucketLogging(genericRequest);
    }

    @Override
    public void setBucketWebsite(SetBucketWebsiteRequest setBucketWebSiteRequest) throws OSSException, ClientException {
        trace("setBucketWebsite: {} {}", setBucketWebSiteRequest.getBucketName(), setBucketWebSiteRequest.getIndexDocument());
        oss.setBucketWebsite(setBucketWebSiteRequest);
    }

    @Override
    public BucketWebsiteResult getBucketWebsite(String bucketName) throws OSSException, ClientException {
        trace("getBucketWebsite: {}", bucketName);
        return oss.getBucketWebsite(bucketName);
    }

    @Override
    public BucketWebsiteResult getBucketWebsite(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketWebsite: {}", genericRequest.getBucketName());
        return oss.getBucketWebsite(genericRequest);
    }

    @Override
    public void deleteBucketWebsite(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketWebsite: {}", bucketName);
        oss.deleteBucketWebsite(bucketName);
    }

    @Override
    public void deleteBucketWebsite(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketWebsite: {}", genericRequest.getBucketName());
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
        trace("setBucketLifecycle: {}", setBucketLifecycleRequest.getBucketName());
        oss.setBucketLifecycle(setBucketLifecycleRequest);
    }

    @Override
    public List<LifecycleRule> getBucketLifecycle(String bucketName) throws OSSException, ClientException {
        trace("getBucketLifecycle: {}", bucketName);
        return oss.getBucketLifecycle(bucketName);
    }

    @Override
    public List<LifecycleRule> getBucketLifecycle(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketLifecycle: {}", genericRequest.getBucketName());
        return oss.getBucketLifecycle(genericRequest);
    }

    @Override
    public void deleteBucketLifecycle(String bucketName) throws OSSException, ClientException {
        trace("deleteBucketLifecycle: {}", bucketName);
        oss.deleteBucketLifecycle(bucketName);
    }

    @Override
    public void deleteBucketLifecycle(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteBucketLifecycle: {}", genericRequest.getBucketName());
        oss.deleteBucketLifecycle(genericRequest);
    }

    @Override
    public void addBucketReplication(AddBucketReplicationRequest addBucketReplicationRequest) throws OSSException, ClientException {
        trace("addBucketReplication: {} {}", addBucketReplicationRequest.getBucketName(), addBucketReplicationRequest.getTargetBucketName());
        oss.addBucketReplication(addBucketReplicationRequest);
    }

    @Override
    public List<ReplicationRule> getBucketReplication(String bucketName) throws OSSException, ClientException {
        trace("getBucketReplication: {}", bucketName);
        return oss.getBucketReplication(bucketName);
    }

    @Override
    public List<ReplicationRule> getBucketReplication(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketReplication: {}", genericRequest.getBucketName());
        return oss.getBucketReplication(genericRequest);
    }

    @Override
    public void deleteBucketReplication(String bucketName, String replicationRuleID) throws OSSException, ClientException {
        trace("deleteBucketReplication: {}", bucketName);
        oss.deleteBucketReplication(bucketName, replicationRuleID);
    }

    @Override
    public void deleteBucketReplication(DeleteBucketReplicationRequest deleteBucketReplicationRequest) throws OSSException, ClientException {
        trace("deleteBucketReplication: {}", deleteBucketReplicationRequest.getBucketName());
        oss.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(String bucketName, String replicationRuleID) throws OSSException, ClientException {
        trace("getBucketReplicationProgress: {}", bucketName);
        return oss.getBucketReplicationProgress(bucketName, replicationRuleID);
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(GetBucketReplicationProgressRequest getBucketReplicationProgressRequest) throws OSSException, ClientException {
        trace("getBucketReplicationProgress: {}", getBucketReplicationProgressRequest.getBucketName());
        return oss.getBucketReplicationProgress(getBucketReplicationProgressRequest);
    }

    @Override
    public List<String> getBucketReplicationLocation(String bucketName) throws OSSException, ClientException {
        trace("getBucketReplicationLocation: {}", bucketName);
        return oss.getBucketReplicationLocation(bucketName);
    }

    @Override
    public List<String> getBucketReplicationLocation(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketReplicationLocation: {}", genericRequest.getBucketName());
        return oss.getBucketReplicationLocation(genericRequest);
    }

    @Override
    public void addBucketCname(AddBucketCnameRequest addBucketCnameRequest) throws OSSException, ClientException {
        trace("addBucketCname: {} {}", addBucketCnameRequest.getBucketName(), addBucketCnameRequest.getDomain());
        oss.addBucketCname(addBucketCnameRequest);
    }

    @Override
    public List<CnameConfiguration> getBucketCname(String bucketName) throws OSSException, ClientException {
        trace("getBucketCname: {}", bucketName);
        return oss.getBucketCname(bucketName);
    }

    @Override
    public List<CnameConfiguration> getBucketCname(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketCname: {}", genericRequest.getBucketName());
        return oss.getBucketCname(genericRequest);
    }

    @Override
    public void deleteBucketCname(String bucketName, String domain) throws OSSException, ClientException {
        trace("deleteBucketCname: {} {}", bucketName, domain);
        oss.deleteBucketCname(bucketName, domain);
    }

    @Override
    public void deleteBucketCname(DeleteBucketCnameRequest deleteBucketCnameRequest) throws OSSException, ClientException {
        trace("deleteBucketCname: {} {}", deleteBucketCnameRequest.getBucketName(), deleteBucketCnameRequest.getDomain());
        oss.deleteBucketCname(deleteBucketCnameRequest);
    }

    @Override
    public BucketInfo getBucketInfo(String bucketName) throws OSSException, ClientException {
        trace("getBucketInfo: {}", bucketName);
        return oss.getBucketInfo(bucketName);
    }

    @Override
    public BucketInfo getBucketInfo(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketInfo: {}", genericRequest.getBucketName());
        return oss.getBucketInfo(genericRequest);
    }

    @Override
    public BucketStat getBucketStat(String bucketName) throws OSSException, ClientException {
        trace("getBucketStat: {}", bucketName);
        return oss.getBucketStat(bucketName);
    }

    @Override
    public BucketStat getBucketStat(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketStat: {}", genericRequest.getBucketName());
        return oss.getBucketStat(genericRequest);
    }

    @Override
    public void setBucketStorageCapacity(String bucketName, UserQos userQos) throws OSSException, ClientException {
        trace("setBucketStorageCapacity: {} {}", bucketName, userQos.getStorageCapacity());
        oss.setBucketStorageCapacity(bucketName, userQos);
    }

    @Override
    public void setBucketStorageCapacity(SetBucketStorageCapacityRequest setBucketStorageCapacityRequest) throws OSSException, ClientException {
        trace("setBucketStorageCapacity: {} {}", setBucketStorageCapacityRequest.getBucketName(), setBucketStorageCapacityRequest.getUserQos().getStorageCapacity());
        oss.setBucketStorageCapacity(setBucketStorageCapacityRequest);
    }

    @Override
    public UserQos getBucketStorageCapacity(String bucketName) throws OSSException, ClientException {
        trace("getBucketStorageCapacity: {}", bucketName);
        return oss.getBucketStorageCapacity(bucketName);
    }

    @Override
    public UserQos getBucketStorageCapacity(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getBucketStorageCapacity: {}", genericRequest.getBucketName());
        return oss.getBucketStorageCapacity(genericRequest);
    }

    @Override
    public UploadFileResult uploadFile(UploadFileRequest uploadFileRequest) throws Throwable {
        trace("uploadFile: {} {} {}", uploadFileRequest.getBucketName(), uploadFileRequest.getKey(), uploadFileRequest.getUploadFile());
        return oss.uploadFile(uploadFileRequest);
    }

    @Override
    public DownloadFileResult downloadFile(DownloadFileRequest downloadFileRequest) throws Throwable {
        trace("downloadFile: {} {} {}", downloadFileRequest.getBucketName(), downloadFileRequest.getKey(), downloadFileRequest.getDownloadFile());
        return oss.downloadFile(downloadFileRequest);
    }

    @Override
    public CreateLiveChannelResult createLiveChannel(CreateLiveChannelRequest createLiveChannelRequest) throws OSSException, ClientException {
        trace("createLiveChannel: {} {}", createLiveChannelRequest.getBucketName(), createLiveChannelRequest.getLiveChannelName());
        return oss.createLiveChannel(createLiveChannelRequest);
    }

    @Override
    public void setLiveChannelStatus(String bucketName, String liveChannel, LiveChannelStatus status) throws OSSException, ClientException {
        trace("setLiveChannelStatus: {} {} {}", bucketName, liveChannel, status);
        oss.setLiveChannelStatus(bucketName, liveChannel, status);
    }

    @Override
    public void setLiveChannelStatus(SetLiveChannelRequest setLiveChannelRequest) throws OSSException, ClientException {
        trace("setLiveChannelStatus: {} {} {}", setLiveChannelRequest.getBucketName(), setLiveChannelRequest.getLiveChannelName(), setLiveChannelRequest.getLiveChannelStatus());
        oss.setLiveChannelStatus(setLiveChannelRequest);
    }

    @Override
    public LiveChannelInfo getLiveChannelInfo(String bucketName, String liveChannel) throws OSSException, ClientException {
        trace("getLiveChannelInfo: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelInfo(bucketName, liveChannel);
    }

    @Override
    public LiveChannelInfo getLiveChannelInfo(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        trace("getLiveChannelInfo: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelInfo(liveChannelGenericRequest);
    }

    @Override
    public LiveChannelStat getLiveChannelStat(String bucketName, String liveChannel) throws OSSException, ClientException {
        trace("getLiveChannelStat: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelStat(bucketName, liveChannel);
    }

    @Override
    public LiveChannelStat getLiveChannelStat(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        trace("getLiveChannelStat: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelStat(liveChannelGenericRequest);
    }

    @Override
    public void deleteLiveChannel(String bucketName, String liveChannel) throws OSSException, ClientException {
        trace("deleteLiveChannel: {} {}", bucketName, liveChannel);
        oss.deleteLiveChannel(bucketName, liveChannel);
    }

    @Override
    public void deleteLiveChannel(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        trace("deleteLiveChannel: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        oss.deleteLiveChannel(liveChannelGenericRequest);
    }

    @Override
    public List<LiveChannel> listLiveChannels(String bucketName) throws OSSException, ClientException {
        trace("listLiveChannels: {}", bucketName);
        return oss.listLiveChannels(bucketName);
    }

    @Override
    public LiveChannelListing listLiveChannels(ListLiveChannelsRequest listLiveChannelRequest) throws OSSException, ClientException {
        trace("listLiveChannels: {}", listLiveChannelRequest.getBucketName());
        return oss.listLiveChannels(listLiveChannelRequest);
    }

    @Override
    public List<LiveRecord> getLiveChannelHistory(String bucketName, String liveChannel) throws OSSException, ClientException {
        trace("getLiveChannelHistory: {} {}", bucketName, liveChannel);
        return oss.getLiveChannelHistory(bucketName, liveChannel);
    }

    @Override
    public List<LiveRecord> getLiveChannelHistory(LiveChannelGenericRequest liveChannelGenericRequest) throws OSSException, ClientException {
        trace("getLiveChannelHistory: {} {}", liveChannelGenericRequest.getBucketName(), liveChannelGenericRequest.getLiveChannelName());
        return oss.getLiveChannelHistory(liveChannelGenericRequest);
    }

    @Override
    public void generateVodPlaylist(String bucketName, String liveChannelName, String PlaylistName, long startTime, long endTime) throws OSSException, ClientException {
        trace("generateVodPlaylist: {} {}", bucketName, liveChannelName);
        oss.generateVodPlaylist(bucketName, liveChannelName, PlaylistName, startTime, endTime);
    }

    @Override
    public void generateVodPlaylist(GenerateVodPlaylistRequest generateVodPlaylistRequest) throws OSSException, ClientException {
        trace("generateVodPlaylist: {} {}", generateVodPlaylistRequest.getBucketName(), generateVodPlaylistRequest.getLiveChannelName());
        oss.generateVodPlaylist(generateVodPlaylistRequest);
    }

    @Override
    public String generateRtmpUri(String bucketName, String liveChannelName, String PlaylistName, long expires) throws OSSException, ClientException {
        trace("generateRtmpUri: {} {}", bucketName, liveChannelName);
        return oss.generateRtmpUri(bucketName, liveChannelName, PlaylistName, expires);
    }

    @Override
    public String generateRtmpUri(GenerateRtmpUriRequest generatePushflowUrlRequest) throws OSSException, ClientException {
        trace("generateRtmpUri: {} {}", generatePushflowUrlRequest.getBucketName(), generatePushflowUrlRequest.getLiveChannelName());
        return oss.generateRtmpUri(generatePushflowUrlRequest);
    }

    @Override
    public void createSymlink(String bucketName, String symlink, String target) throws OSSException, ClientException {
        trace("createSymlink: {} {} {}", bucketName, symlink, target);
        oss.createSymlink(bucketName, symlink, target);
    }

    @Override
    public void createSymlink(CreateSymlinkRequest createSymlinkRequest) throws OSSException, ClientException {
        trace("createSymlink: {} {} {}", createSymlinkRequest.getBucketName(), createSymlinkRequest.getSymlink(), createSymlinkRequest.getTarget());
        oss.createSymlink(createSymlinkRequest);
    }

    @Override
    public OSSSymlink getSymlink(String bucketName, String symlink) throws OSSException, ClientException {
        trace("getSymlink: {} {}", bucketName, symlink);
        return oss.getSymlink(bucketName, symlink);
    }

    @Override
    public OSSSymlink getSymlink(GenericRequest genericRequest) throws OSSException, ClientException {
        trace("getSymlink: {} {}", genericRequest.getBucketName(), genericRequest.getKey());
        return oss.getSymlink(genericRequest);
    }

    @Override
    public GenericResult processObject(ProcessObjectRequest processObjectRequest) throws OSSException, ClientException {
        trace("processObjectRequest: {} {} {}", processObjectRequest.getBucketName(), processObjectRequest.getKey(), processObjectRequest.getProcess());
        return oss.processObject(processObjectRequest);
    }

    @Override
    public void createUdf(CreateUdfRequest createUdfRequest) throws OSSException, ClientException {
        trace("createUdf: {}", createUdfRequest.getName());
        oss.createUdf(createUdfRequest);
    }

    @Override
    public UdfInfo getUdfInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("getUdfInfo: {}", genericRequest.getName());
        return oss.getUdfInfo(genericRequest);
    }

    @Override
    public List<UdfInfo> listUdfs() throws OSSException, ClientException {
        trace("listUdfs");
        return oss.listUdfs();
    }

    @Override
    public void deleteUdf(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteUdf: {}", genericRequest.getName());
        oss.deleteUdf(genericRequest);
    }

    @Override
    public void uploadUdfImage(UploadUdfImageRequest uploadUdfImageRequest) throws OSSException, ClientException {
        trace("uploadUdfImage: {}", uploadUdfImageRequest.getName());
        oss.uploadUdfImage(uploadUdfImageRequest);
    }

    @Override
    public List<UdfImageInfo> getUdfImageInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("getUdfImageInfo: {}", genericRequest.getName());
        return oss.getUdfImageInfo(genericRequest);
    }

    @Override
    public void deleteUdfImage(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteUdfImage: {}", genericRequest.getName());
        oss.deleteUdfImage(genericRequest);
    }

    @Override
    public void createUdfApplication(CreateUdfApplicationRequest createUdfApplicationRequest) throws OSSException, ClientException {
        trace("createUdfApplication: {}", createUdfApplicationRequest.getName());
        oss.createUdfApplication(createUdfApplicationRequest);
    }

    @Override
    public UdfApplicationInfo getUdfApplicationInfo(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("getUdfApplicationInfo: {}", genericRequest.getName());
        return oss.getUdfApplicationInfo(genericRequest);
    }

    @Override
    public List<UdfApplicationInfo> listUdfApplications() throws OSSException, ClientException {
        trace("listUdfApplications");
        return oss.listUdfApplications();
    }

    @Override
    public void deleteUdfApplication(UdfGenericRequest genericRequest) throws OSSException, ClientException {
        trace("deleteUdfApplication: {}", genericRequest.getName());
        oss.deleteUdfApplication(genericRequest);
    }

    @Override
    public void upgradeUdfApplication(UpgradeUdfApplicationRequest upgradeUdfApplicationRequest) throws OSSException, ClientException {
        trace("upgradeUdfApplication: {}", upgradeUdfApplicationRequest.getName());
        oss.upgradeUdfApplication(upgradeUdfApplicationRequest);
    }

    @Override
    public void resizeUdfApplication(ResizeUdfApplicationRequest resizeUdfApplicationRequest) throws OSSException, ClientException {
        trace("resizeUdfApplication: {}", resizeUdfApplicationRequest.getName());
        oss.resizeUdfApplication(resizeUdfApplicationRequest);
    }

    @Override
    public UdfApplicationLog getUdfApplicationLog(GetUdfApplicationLogRequest getUdfApplicationLogRequest) throws OSSException, ClientException {
        trace("getUdfApplicationLog: {}", getUdfApplicationLogRequest.getName());
        return oss.getUdfApplicationLog(getUdfApplicationLogRequest);
    }
}
