package cc.whohow.aliyun.oss.vfs;

import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.commons.vfs2.FileSystemException;

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

    @Override
    public Object getAttribute(String attrName) throws FileSystemException {
        switch (attrName) {
            case OSSHeaders.ETAG:
                return ossObjectSummary.getETag();
            case OSSHeaders.CONTENT_LENGTH:
                return ossObjectSummary.getSize();
            case OSSHeaders.LAST_MODIFIED:
                return ossObjectSummary.getLastModified();
            default:
                return super.getAttribute(attrName);
        }
    }
}
