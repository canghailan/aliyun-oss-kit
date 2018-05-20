package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.diff.Diff;
import cc.whohow.aliyun.oss.diff.DiffStatus;
import cc.whohow.aliyun.oss.file.FileTree;
import cc.whohow.aliyun.oss.tree.TreePostOrderIterator;
import cc.whohow.aliyun.oss.tree.TreePreOrderIterator;
import com.aliyun.oss.model.OSSObjectSummary;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * 阿里云OSS同步工具
 */
public class AliyunOSSSynchronizer {
    private Diff<String, String> diff = new Diff<>();

    /**
     * 文件ETag(MD5)
     */
    public String getETag(File file) {
        try (InputStream stream = new FileInputStream(file)) {
            return DigestUtils.md5Hex(stream).toUpperCase();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 目录下所有文件ETag
     */
    public Map<String, String> getETags(File directory) {
        URI directoryUri = directory.toURI();
        Map<String, String> eTags = new TreeMap<>();
        for (File file : new FileTree(directory, TreePreOrderIterator::new)) {
            String relative = directoryUri.relativize(file.toURI()).getPath();
            eTags.put(relative, getETag(file));
        }
        return eTags;
    }

    /**
     * 阿里云OSS目录下所有对象ETag
     */
    public Map<String, String> getETags(AliyunOSSObject directory) {
        Map<String, String> eTags = new TreeMap<>();
        AliyunOSSObjectSummaryIterator iterator = directory.listObjectSummariesRecursively();
        while (iterator.hasNext()) {
            OSSObjectSummary objectSummary = iterator.next();
            String relative = objectSummary.getKey().substring(directory.getKey().length());
            eTags.put(relative, objectSummary.getETag());
        }
        return eTags;
    }

    /**
     * 解析相对地址
     */
    protected File resolve(File file, String relative) {
        return new File(file, relative);
    }

    /**
     * 解析相对地址
     */
    protected AliyunOSSObject resolve(AliyunOSSObject object, String relative) {
        return new AliyunOSSObject(object.getOSS(), object.getBucketName(), object.getKey() + relative);
    }

    /**
     * 同步本地文件夹到OSS
     */
    public Map<String, DiffStatus> synchronize(File source, AliyunOSSObject target) {
        Map<String, DiffStatus> diffs = diff.diff(getETags(target), getETags(source));
        for (Map.Entry<String, DiffStatus> diff : diffs.entrySet()) {
            AliyunOSSObject targetObject = resolve(target, diff.getKey());
            switch (diff.getValue()) {
                case ADDED:
                case MODIFIED: {
                    targetObject.putObject(resolve(source, diff.getKey()));
                    break;
                }
                case DELETED: {
                    targetObject.deleteObject();
                    break;
                }
                default: {
                    break;
                }
            }
        }
        return diffs;
    }

    /**
     * 同步OSS文件夹到本地
     */
    public Map<String, DiffStatus> synchronize(AliyunOSSObject source, File target) {
        Map<String, DiffStatus> diffs = diff.diff(getETags(target), getETags(source));
        for (Map.Entry<String, DiffStatus> diff : diffs.entrySet()) {
            File targetFile = resolve(target, diff.getKey());
            switch (diff.getValue()) {
                case ADDED:
                case MODIFIED: {
                    targetFile.getParentFile().mkdirs();
                    resolve(source, diff.getKey()).getObject(targetFile);
                    break;
                }
                case DELETED: {
                    targetFile.delete();
                    break;
                }
                default: {
                    break;
                }
            }
        }
        for (File file : new FileTree(target, TreePostOrderIterator::new)) {
            File[] files = file.listFiles();
            if (files != null && files.length == 0) {
                file.delete();
            }
        }
        return diffs;
    }

    /**
     * 同步两个 OSS 文件夹
     */
    public Map<String, DiffStatus> synchronize(AliyunOSSObject source, AliyunOSSObject target) {
        Map<String, DiffStatus> diffs = diff.diff(getETags(target), getETags(source));
        BiFunction<AliyunOSSObject, AliyunOSSObject, ?> copy = target.isCopyable(source) ?
                AliyunOSSObject::copyFromObject : AliyunOSSObject::putObject;
        for (Map.Entry<String, DiffStatus> diff : diffs.entrySet()) {
            AliyunOSSObject targetObject = resolve(target, diff.getKey());
            switch (diff.getValue()) {
                case ADDED:
                case MODIFIED: {
                    copy.apply(targetObject, resolve(source, diff.getKey()));
                    break;
                }
                case DELETED: {
                    targetObject.deleteObject();
                    break;
                }
                default: {
                    break;
                }
            }
        }
        return diffs;
    }
}
