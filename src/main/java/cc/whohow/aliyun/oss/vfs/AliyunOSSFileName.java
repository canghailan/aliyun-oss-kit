package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSUri;
import cc.whohow.vfs.SimpleFileName;
import cc.whohow.vfs.path.PathBuilder;
import cc.whohow.vfs.path.PathParser;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;

import java.net.URI;

/**
 * 阿里云文件名
 *
 * @see com.aliyun.oss.model.OSSObject
 * @see com.aliyun.oss.model.Bucket
 */
public class AliyunOSSFileName extends AliyunOSSUri implements SimpleFileName {
    public AliyunOSSFileName(String uri) {
        super(uri);
    }

    public AliyunOSSFileName(URI uri) {
        super(uri);
    }

    public AliyunOSSFileName(String bucketName, String key) {
        super(null, null, bucketName, null, key);
    }

    /**
     * 路径
     */
    public String getPath() {
        return SEPARATOR + getKey();
    }

    /**
     * 协议
     */
    public String getScheme() {
        return "oss";
    }

    /**
     * URI
     */
    public String getURI() {
        return toString();
    }

    /**
     * 根地址URL
     */
    public String getRootURI() {
        return "oss://" + getBucketName() + "/";
    }

    /**
     * 根目录
     */
    public AliyunOSSFileName getRoot() {
        return new AliyunOSSFileName(getBucketName(), "");
    }

    /**
     * 上级目录
     */
    public AliyunOSSFileName getParent() {
        String prefix = new PathParser(key).getParent();
        if (prefix == null) {
            return null;
        }
        return new AliyunOSSFileName(getBucketName(), prefix);
    }

    /**
     * 解析相对路径
     */
    public AliyunOSSFileName resolveChild(String child) {
        return new AliyunOSSFileName(getBucketName(), getKey() + child);
    }

    /**
     * 解析相对路径
     */
    public AliyunOSSFileName resolveRelative(String relative) {
        String newKey = builder()
                .resolve(relative)
                .startsWithSeparator(false)
                .toString();
        return new AliyunOSSFileName(getBucketName(), newKey);
    }

    /**
     * 相对路径
     */
    public String getRelativeName(FileName name) throws FileSystemException {
        if (name instanceof AliyunOSSFileName) {
            AliyunOSSFileName that = (AliyunOSSFileName) name;
            if (!that.getBucketName().equals(getBucketName())) {
                throw new FileSystemException("");
            }

            return builder()
                    .relativize(that.builder())
                    .toString();
        }
        throw new FileSystemException("");
    }

    /**
     * 是否是下级文件列表，过滤
     */
    public boolean isDescendent(FileName descendant, NameScope nameScope) {
        if (descendant instanceof AliyunOSSFileName) {
            AliyunOSSFileName descendantName = (AliyunOSSFileName) descendant;
            return descendantName.getBucketName().equals(getBucketName()) &&
                    descendantName.getKey().startsWith(getKey());
        }
        return false;
    }

    /**
     * 获取文件类型
     */
    public FileType getType() {
        if (key.isEmpty() || key.endsWith(SEPARATOR)) {
            return FileType.FOLDER;
        } else {
            return FileType.FILE;
        }
    }

    public PathBuilder builder() {
        return new PathBuilder(getKey())
                .startsWithSeparator(true)
                .endsWithSeparator(getType() == FileType.FOLDER);
    }
}
