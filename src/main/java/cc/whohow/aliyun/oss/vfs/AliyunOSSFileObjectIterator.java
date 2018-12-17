package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSObjectListingIterator;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * VFS 文件遍历器
 * TODO 优化
 */
public class AliyunOSSFileObjectIterator implements Iterator<FileObject> {
    private final AliyunOSSFileObject baseFolder;
    private final AliyunOSSObjectListingIterator iterator;
    private final boolean listFile;
    private final boolean listFolder;
    private Iterator<String> commonPrefixIterator;
    private Iterator<OSSObjectSummary> objectSummaryIterator;
    private AliyunOSSFileObject fileObject;

    public static Iterator<FileObject> create(AliyunOSSFileObject baseFolder, boolean recursively) {
        return new AliyunOSSFileObjectIterator(baseFolder, recursively, true, true);
    }

    public static Iterator<FileObject> create(AliyunOSSFileObject baseFolder,
            boolean recursively,
            boolean listFile,
            boolean listFolder) {
        return new AliyunOSSFileObjectIterator(baseFolder, recursively, listFile, listFolder);
    }

    private AliyunOSSFileObjectIterator(
            AliyunOSSFileObject baseFolder,
            boolean recursively,
            boolean listFile,
            boolean listFolder) {
        this.baseFolder = baseFolder;
        this.iterator = new AliyunOSSObjectListingIterator(
                baseFolder.getOSS(), baseFolder.getBucketName(), baseFolder.getKey(), recursively ? null : "/");
        this.listFile = listFile;
        this.listFolder = listFolder;
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
            commonPrefixIterator = listFolder ?
                    objectListing.getCommonPrefixes().iterator() :
                    Collections.emptyIterator();
            objectSummaryIterator = listFile ?
                    objectListing.getObjectSummaries().stream()
                            .filter(o -> !o.getKey().endsWith("/"))
                            .iterator() :
                    Collections.emptyIterator();
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
        try {
            fileObject.deleteAll();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    private AliyunOSSFileObject newFileObject(String commonPrefix) {
        return new AliyunOSSFileObject(baseFolder.getFileSystem(),
                new AliyunOSSFileName(baseFolder.getBucketName(), commonPrefix));
    }

    private AliyunOSSObjectSummaryFileObject newFileObject(OSSObjectSummary objectSummary) {
        return new AliyunOSSObjectSummaryFileObject(baseFolder.getFileSystem(), objectSummary);
    }
}
