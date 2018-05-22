package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.diff.Diff;
import cc.whohow.aliyun.oss.diff.DiffStatus;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObjectSummary;

import java.util.Collections;
import java.util.Map;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 阿里云OSS目录文件变化监听任务
 */
public class AliyunOSSWatcher implements Callable<Map<String, DiffStatus>> {
    private static final Diff<String, String> DIFF = new Diff<>();

    private final AliyunOSSObject object;
    private volatile Map<String, String> eTags;

    public AliyunOSSWatcher(AliyunOSSObject object) {
        this.object = object;
    }

    public OSS getOSS() {
        return object.getOSS();
    }

    public String getBucketName() {
        return object.getBucketName();
    }

    public String getKey() {
        return object.getKey();
    }

    /**
     * 比较并返回结果
     */
    public synchronized Map<String, DiffStatus> call() {
        try {
            Map<String, String> oldETags = eTags;
            Map<String, String> newETags = getETags();
            eTags = newETags;

            if (oldETags == null) {
                return Collections.emptyMap();
            }
            return DIFF.diff(oldETags, newETags);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * 获取文件快照
     */
    private Map<String, String> getETags() {
        if (getKey().isEmpty() || getKey().endsWith("/")) {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    object.listObjectSummariesRecursively(), 0), false)
                    .collect(Collectors.toMap(OSSObjectSummary::getKey, OSSObjectSummary::getETag));
        } else {
            Map<String, String> eTags = new TreeMap<>();
            try {
                eTags.put(object.getKey(), object.getSimplifiedObjectMeta().getETag());
                return eTags;
            } catch (OSSException e) {
                if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
                    return eTags;
                }
                throw e;
            }
        }
    }
}
