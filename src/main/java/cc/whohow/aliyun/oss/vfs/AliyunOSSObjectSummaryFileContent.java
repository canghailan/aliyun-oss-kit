package cc.whohow.aliyun.oss.vfs;

import com.aliyun.oss.model.OSSObjectSummary;

public class AliyunOSSObjectSummaryFileContent extends AliyunOSSFileContent {
    protected final OSSObjectSummary ossObjectSummary;

    public AliyunOSSObjectSummaryFileContent(AliyunOSSFileObject file, OSSObjectSummary ossObjectSummary) {
        super(file);
        this.ossObjectSummary = ossObjectSummary;
    }

    @Override
    public long getSize() {
        return ossObjectSummary.getSize();
    }

    @Override
    public long getLastModifiedTime() {
        return ossObjectSummary.getLastModified().getTime();
    }
}
