package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.io.Java9InputStream;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;
import org.apache.commons.vfs2.util.RandomAccessMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 阿里云文件内容
 */
public class AliyunOSSFileContent implements FileContent {
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
    public boolean hasAttribute(String attrName) {
        return getAttributes().containsKey(attrName);
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
    public String[] getAttributeNames() {
        return getAttributes().keySet().toArray(new String[0]);
    }

    @Override
    public Object getAttribute(String attrName) {
        return getAttributes().get(attrName);
    }

    @Override
    public void setAttribute(String attrName, Object value) {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        objectMetadata.addUserMetadata(attrName, String.valueOf(value));
        file.setObjectMetadata(objectMetadata);
    }

    @Override
    public void removeAttribute(String attrName) {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        objectMetadata.getUserMetadata().remove(attrName);
        file.setObjectMetadata(objectMetadata);
    }

    @Override
    public Certificate[] getCertificates() {
        return new Certificate[0];
    }


    @Override
    public InputStream getInputStream() {
        return file.getObjectContent();
    }

    @Override
    public OutputStream getOutputStream() {
        return file.appendObject();
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
    public void close() {
        // do nothing
    }

    @Override
    public FileContentInfo getContentInfo() {
        ObjectMetadata objectMetadata = file.getObjectMetadata();
        return new DefaultFileContentInfo(objectMetadata.getContentType(), objectMetadata.getContentEncoding());
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public long write(FileContent output) throws IOException {
        if (output instanceof AliyunOSSFileContent) {
            AliyunOSSFileObject outputFileObject = ((AliyunOSSFileContent) output).getFile();
            if (outputFileObject.isCopyable(getFile())) {
                outputFileObject.copyFromObject(getFile());
                return getSize();
            } else {
                try (OSSObject object = file.getObject()) {
                    outputFileObject.putObject(object.getObjectContent(), object.getObjectMetadata());
                    return object.getObjectMetadata().getContentLength();
                }
            }
        } else {
            try (OutputStream stream = output.getOutputStream()) {
                return write(stream);
            }
        }
    }

    @Override
    public long write(FileObject file) throws IOException {
        try (FileContent output = file.getContent()) {
            return write(output);
        }
    }

    @Override
    public long write(OutputStream output) throws IOException {
        return write(output, 8 * 1024);
    }

    @Override
    public long write(OutputStream output, int bufferSize) throws IOException {
        try (Java9InputStream input = new Java9InputStream(getInputStream())) {
            return input.transferTo(output, new byte[bufferSize]);
        }
    }
}
