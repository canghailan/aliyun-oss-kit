package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSObjectMetadata;
import cc.whohow.vfs.SimpleFileContent;
import cc.whohow.vfs.StatelessFileContent;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileContentInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;
import org.apache.commons.vfs2.util.RandomAccessMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 阿里云文件内容
 */
public class AliyunOSSFileContent implements SimpleFileContent, StatelessFileContent {
    protected final AliyunOSSFileObject file;

    public AliyunOSSFileContent(AliyunOSSFileObject file) {
        this.file = file;
    }

    @Override
    public AliyunOSSFileObject getFile() {
        return file;
    }

    @Override
    public long getSize() {
        return file.getSimplifiedObjectMeta().getSize();
    }

    @Override
    public long getLastModifiedTime() {
        return file.getSimplifiedObjectMeta().getLastModified().getTime();
    }

    @Override
    public void setLastModifiedTime(long modTime) {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        objectMetadata.setLastModified(new Date(modTime));
        file.setObjectMetadata(objectMetadata);
    }

    @Override
    public Map<String, Object> getAttributes() {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.putAll(objectMetadata.getRawMetadata());
        attributes.putAll(objectMetadata.getUserMetadata());
        return attributes;
    }

    @Override
    public boolean hasAttribute(String attrName) {
        return getAttributes().containsKey(AliyunOSSObjectMetadata.normalizeName(attrName));
    }

    @Override
    public Object getAttribute(String attrName) throws FileSystemException {
        return getAttributes().get(AliyunOSSObjectMetadata.normalizeName(attrName));
    }

    @Override
    public void setAttribute(String attrName, Object value) {
        attrName = AliyunOSSObjectMetadata.normalizeName(attrName);
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        if (AliyunOSSObjectMetadata.isRawMetaData(attrName)) {
            objectMetadata.setHeader(attrName, value);
        } else {
            objectMetadata.addUserMetadata(attrName, String.valueOf(value));
        }
        file.setObjectMetadata(objectMetadata);
    }

    @Override
    public void removeAttribute(String attrName) {
        attrName = AliyunOSSObjectMetadata.normalizeName(attrName);
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        if (AliyunOSSObjectMetadata.isRawMetaData(attrName)) {
            objectMetadata.setHeader(attrName, null);
        } else {
            objectMetadata.getUserMetadata().remove(attrName);
        }
        file.setObjectMetadata(objectMetadata);
    }


    @Override
    public InputStream getInputStream() {
        return file.getObjectContent();
    }

    @Override
    public RandomAccessContent getRandomAccessContent(RandomAccessMode mode) throws FileSystemException {
        throw new FileSystemException("vfs.provider/random-access-not-supported.error");
    }

    @Override
    public OutputStream getOutputStream(boolean bAppend) {
        if (bAppend) {
            return file.appendObject(getSize());
        } else {
            return file.appendObject();
        }
    }

    @Override
    public FileContentInfo getContentInfo() {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        return new DefaultFileContentInfo(objectMetadata.getContentType(), objectMetadata.getContentEncoding());
    }

    private long write(AliyunOSSFileContent fileContent) throws IOException {
        fileContent.getFile().copyFile(getFile());
        return getSize();
    }

    @Override
    public long write(FileContent output) throws IOException {
        if (output instanceof AliyunOSSFileContent) {
            return write((AliyunOSSFileContent) output);
        } else {
            try (OutputStream stream = output.getOutputStream()) {
                return write(stream);
            }
        }
    }
}
