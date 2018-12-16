package cc.whohow.aliyun.oss.vfs;

import cc.whohow.vfs.version.FileAttributeVersionProvider;
import com.aliyun.oss.internal.OSSHeaders;

public class AliyunOSSFileVersionProvider extends FileAttributeVersionProvider {
    public AliyunOSSFileVersionProvider() {
        super(OSSHeaders.ETAG);
    }
}
