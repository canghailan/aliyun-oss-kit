package cc.whohow.aliyun.oss.vfs;

import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.commons.vfs2.FileSystem;


public class AliyunOSSObjectSummaryFileObject extends AliyunOSSFileObject {
    protected final OSSObjectSummary ossObjectSummary;

    public AliyunOSSObjectSummaryFileObject(AliyunOSSFileSystem fileSystem, OSSObjectSummary ossObjectSummary) {
        super(fileSystem, new AliyunOSSFileName(ossObjectSummary.getBucketName(), ossObjectSummary.getKey()));
        this.ossObjectSummary = ossObjectSummary;
    }

    public OSSObjectSummary getOSSObjectSummary() {
        return ossObjectSummary;
    }

    @Override
    public AliyunOSSObjectSummaryFileContent getContent() {
        return new AliyunOSSObjectSummaryFileContent(this, ossObjectSummary);
    }
}
