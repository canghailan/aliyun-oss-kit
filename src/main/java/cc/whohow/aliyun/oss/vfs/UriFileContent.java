package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.io.Java9InputStream;
import cc.whohow.aliyun.oss.io.ResourceInputStream;
import cc.whohow.aliyun.oss.net.ApacheHttpClient;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.DateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class UriFileContent implements FileContent {
    private final UriFileObject file;

    public UriFileContent(UriFileObject file) {
        this.file = file;
    }

    @Override
    public FileObject getFile() {
        return file;
    }

    @Override
    public long getSize() throws FileSystemException {
        Object contentLength = getAttribute("content-length");
        return contentLength == null ? -1 : Long.parseLong(contentLength.toString());
    }

    @Override
    public long getLastModifiedTime() throws FileSystemException {
        Object lastModified = getAttribute("last-modified");
        return lastModified == null ? -1 : DateUtils.parseDate(lastModified.toString()).getTime();
    }

    @Override
    public void setLastModifiedTime(long modTime) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean hasAttribute(String attrName) throws FileSystemException {
        return getAttributes().containsKey(attrName);
    }

    @Override
    public Map<String, Object> getAttributes() throws FileSystemException {
        try (CloseableHttpResponse response = ApacheHttpClient.get().execute(new HttpGet(file.getURL().toString()))) {
            Map<String, Object> attributes = new LinkedHashMap<>();
            for (Header header : response.getAllHeaders()) {
                attributes.put(header.getName().toLowerCase(), header.getValue());
            }
            return attributes;
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
    }

    @Override
    public String[] getAttributeNames() throws FileSystemException {
        return getAttributes().keySet().toArray(new String[0]);
    }

    @Override
    public Object getAttribute(String attrName) throws FileSystemException {
        return getAttributes().get(attrName.toLowerCase());
    }

    @Override
    public void setAttribute(String attrName, Object value) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void removeAttribute(String attrName) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public Certificate[] getCertificates() {
        return new Certificate[0];
    }

    @Override
    public InputStream getInputStream() throws FileSystemException {
        try {
            CloseableHttpResponse response = ApacheHttpClient.get().execute(new HttpGet(file.getURL().toString()));
            return new ResourceInputStream(response, response.getEntity().getContent());
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
    }

    @Override
    public OutputStream getOutputStream() throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public RandomAccessContent getRandomAccessContent(RandomAccessMode mode) throws FileSystemException {
        throw new FileSystemException("vfs.provider/random-access-not-supported.error");
    }

    @Override
    public OutputStream getOutputStream(boolean bAppend) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public FileContentInfo getContentInfo() throws FileSystemException {
        Map<String, Object> attributes = getAttributes();
        Object contentType = attributes.get("content-type");
        Object contentEncoding = attributes.get("content-type");
        return new DefaultFileContentInfo(
                Objects.toString(contentType, null),
                Objects.toString(contentEncoding, null));
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public long write(FileContent output) throws IOException {
        try (OutputStream stream = output.getOutputStream()) {
            return write(stream);
        }
    }

    @Override
    public long write(FileObject file) throws IOException {
        try (FileContent fileContent = file.getContent()) {
            return write(fileContent);
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
