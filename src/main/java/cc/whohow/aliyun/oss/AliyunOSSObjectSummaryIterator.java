package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObjectSummary;

import java.util.Collections;
import java.util.Iterator;

/**
 * ObjectSummary 遍历器
 */
public class AliyunOSSObjectSummaryIterator implements Iterator<OSSObjectSummary> {
    private AliyunOSSObjectListingIterator objectListingIterator;
    private Iterator<OSSObjectSummary> objectSummaryIterator;
    private OSSObjectSummary objectSummary;

    public AliyunOSSObjectSummaryIterator(AliyunOSSObjectListingIterator iterator) {
        this.objectListingIterator = iterator;
        this.objectSummaryIterator = Collections.emptyIterator();
    }

    public OSS getOSS() {
        return objectListingIterator.getOSS();
    }

    public String getBucketName() {
        return objectListingIterator.getBucketName();
    }

    public String getPrefix() {
        return objectListingIterator.getPrefix();
    }

    @Override
    public boolean hasNext() {
        if (objectSummaryIterator.hasNext()) {
            return true;
        }
        if (objectListingIterator.hasNext()) {
            objectSummaryIterator = objectListingIterator.next().getObjectSummaries().iterator();
            return hasNext();
        }
        return false;
    }

    @Override
    public OSSObjectSummary next() {
        return objectSummary = objectSummaryIterator.next();
    }

    @Override
    public void remove() {
        getOSS().deleteObject(objectSummary.getBucketName(), objectSummary.getKey());
    }
}
