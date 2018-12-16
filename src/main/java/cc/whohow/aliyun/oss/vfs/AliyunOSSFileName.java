package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSUri;
import cc.whohow.vfs.path.NameIterator;
import cc.whohow.vfs.path.PathBuilder;
import cc.whohow.vfs.path.PathParser;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

/**
 * 阿里云文件名
 *
 * @see com.aliyun.oss.model.OSSObject
 * @see com.aliyun.oss.model.Bucket
 */
public class AliyunOSSFileName extends AliyunOSSUri implements FileName, Iterable<CharSequence> {
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
     * 文件类型
     */
    public static FileType getFileType(String key) {
        if (key.isEmpty() || key.endsWith(SEPARATOR)) {
            return FileType.FOLDER;
        } else {
            return FileType.FILE;
        }
    }

    /**
     * 是否是相对路径
     */
    public static boolean isRelative(String path) {
        return isRelative(URI.create(path));
    }

    /**
     * 是否是相对路径
     */
    public static boolean isRelative(URI uri) {
        return uri.getScheme() == null &&
                uri.getHost() == null &&
                uri.getPath() != null &&
                !uri.getPath().startsWith(FileName.SEPARATOR);
    }

    /**
     * 文件名
     */
    public String getBaseName() {
        return newParser().getLastName();
    }

    /**
     * 路径
     */
    public String getPath() {
        return SEPARATOR + getKey();
    }

    /**
     * 路径
     */
    public String getPathDecoded() {
        return getPath();
    }

    /**
     * 扩展名，不含.
     */
    public String getExtension() {
        return newParser().getExtension();
    }

    /**
     * 名称数
     */
    public int getDepth() {
        return newParser().getNameCount();
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
        String parent = newParser().getParent();
        if (parent == null) {
            return null;
        }
        return new AliyunOSSFileName(getBucketName(), parent);
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
        String newKey = newBuilder().resolve(relative).startsWithSeparator(false).toString();
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

            return newBuilder()
                    .relativize(that.newBuilder())
                    .toString();
        }
        throw new FileSystemException("");
    }

    /**
     * 是否是祖先
     */
    public boolean isAncestor(FileName ancestor) {
        if (ancestor instanceof AliyunOSSFileName) {
            AliyunOSSFileName ancestorName = (AliyunOSSFileName) ancestor;
            return ancestorName.getBucketName().equals(getBucketName()) &&
                    getKey().startsWith(ancestorName.getKey());
        }
        return false;
    }

    /**
     * 是否是下级文件列表
     */
    public boolean isDescendent(FileName descendant) {
        if (descendant instanceof AliyunOSSFileName) {
            AliyunOSSFileName descendantName = (AliyunOSSFileName) descendant;
            return descendantName.getBucketName().equals(getBucketName()) &&
                    descendantName.getKey().startsWith(getKey());
        }
        return false;
    }

    /**
     * 是否是下级文件列表，过滤
     */
    public boolean isDescendent(FileName descendant, NameScope nameScope) {
        return isDescendent(descendant);
    }

    /**
     * 是否是文件
     */
    public boolean isFile() {
        return getType() == FileType.FILE;
    }

    /**
     * 获取文件类型
     */
    public FileType getType() {
        return getFileType(getKey());
    }

    /**
     * 获取URI
     */
    public String getFriendlyURI() {
        return toString();
    }

    /**
     * 比较
     */
    public int compareTo(FileName o) {
        return toString().compareTo(o.toString());
    }

    /**
     * 名称列表
     */
    @Override
    public Iterator<CharSequence> iterator() {
        if (getKey().isEmpty()) {
            return Collections.emptyIterator();
        }
        return new NameIterator(getKey(), SEPARATOR_CHAR);
    }

    private PathParser newParser() {
        return new PathParser(getKey());
    }

    private PathBuilder newBuilder() {
        return new PathBuilder(getKey())
                .startsWithSeparator(true)
                .endsWithSeparator(getType() == FileType.FOLDER);
    }
}
