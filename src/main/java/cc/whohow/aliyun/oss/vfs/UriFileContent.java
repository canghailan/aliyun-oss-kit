package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.io.Java9InputStream;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;
import org.apache.commons.vfs2.util.RandomAccessMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URLConnection;
import java.security.cert.Certificate;
import java.util.LinkedHashMap;
import java.util.Map;

public class UriFileContent implements FileContent {
    private final UriFileObject file;
    private final URLConnection connection;

    public UriFileContent(UriFileObject file) {
        try {
            this.file = file;
            this.connection = file.getURL().openConnection();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public FileObject getFile() {
        return file;
    }

    @Override
    public long getSize() {
        return connection.getContentLengthLong();
    }

    @Override
    public long getLastModifiedTime() {
        return connection.getLastModified();
    }

    @Override
    public void setLastModifiedTime(long modTime) throws FileSystemException {
        throw new FileSystemException("");
    }

    @Override
    public boolean hasAttribute(String attrName) {
        return getAttributes().containsKey(attrName);
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (String name : connection.getHeaderFields().keySet()) {
            attributes.put(name, connection.getHeaderField(name));
        }
        return attributes;
    }

    @Override
    public String[] getAttributeNames()  {
        return connection.getHeaderFields().keySet().toArray(new String[0]);
    }

    @Override
    public Object getAttribute(String attrName)  {
        return connection.getHeaderField(attrName);
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
    public Certificate[] getCertificates()  {
        return new Certificate[0];
    }

    @Override
    public InputStream getInputStream() throws FileSystemException {
        try {
            return connection.getInputStream();
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
    public FileContentInfo getContentInfo() {
        return new DefaultFileContentInfo(connection.getContentType(), connection.getContentEncoding());
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
