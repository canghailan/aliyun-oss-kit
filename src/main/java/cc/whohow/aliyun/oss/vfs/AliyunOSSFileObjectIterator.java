package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSObjectListingIterator;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.vfs2.FileObject;

import java.util.Collections;
import java.util.Iterator;

/**
 * VFS 文件遍历器
 */
public class AliyunOSSFileObjectIterator implements Iterator<FileObject> {
    private AliyunOSSFileObject baseFolder;
    private AliyunOSSObjectListingIterator iterator;
    private Iterator<String> commonPrefixIterator;
    private Iterator<OSSObjectSummary> objectSummaryIterator;
    private AliyunOSSFileObject fileObject;

    public AliyunOSSFileObjectIterator(AliyunOSSFileObject baseFolder, boolean recursively) {
        if (baseFolder.isFile()) {
            throw new IllegalArgumentException();
        }
        this.baseFolder = baseFolder;
        this.iterator = new AliyunOSSObjectListingIterator(
                baseFolder.getOSS(), baseFolder.getBucketName(), baseFolder.getKey(), recursively ? null : "/");
        this.commonPrefixIterator = Collections.emptyIterator();
        this.objectSummaryIterator = Collections.emptyIterator();
    }

    public AliyunOSSFileObject getBaseFolder() {
        return baseFolder;
    }

    @Override
    public boolean hasNext() {
        if (commonPrefixIterator.hasNext()) {
            fileObject = newFileObject(commonPrefixIterator.next());
            return true;
        }
        if (objectSummaryIterator.hasNext()) {
            fileObject = newFileObject(objectSummaryIterator.next());
            return true;
        }
        if (iterator.hasNext()) {
            ObjectListing objectListing = iterator.next();
            commonPrefixIterator = objectListing.getCommonPrefixes().iterator();
            objectSummaryIterator = objectListing.getObjectSummaries().iterator();
            return hasNext();
        }
        return false;
    }

    @Override
    public AliyunOSSFileObject next() {
        return fileObject;
    }

    @Override
    public void remove() {
        fileObject.delete();
    }

    private AliyunOSSFileObject newFileObject(String commonPrefix) {
        return new AliyunOSSFileObject(baseFolder.getFileSystem(),
                new AliyunOSSFileName(baseFolder.getBucketName(), commonPrefix));
    }

    private AliyunOSSObjectSummaryFileObject newFileObject(OSSObjectSummary objectSummary) {
        return new AliyunOSSObjectSummaryFileObject(baseFolder.getFileSystem(), objectSummary);
    }
}
