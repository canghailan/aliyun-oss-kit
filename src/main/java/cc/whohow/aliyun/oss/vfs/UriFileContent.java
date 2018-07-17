package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.io.Java9InputStream;
import cc.whohow.aliyun.oss.io.ResourceInputStream;
import cc.whohow.aliyun.oss.net.HttpURLConnection;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileContentInfo;
import org.apache.commons.vfs2.util.RandomAccessMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.stream.Collectors;

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
        try (HttpURLConnection connection = new HttpURLConnection(file.getURL())) {
            return connection.getContentLengthLong();
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
    }

    @Override
    public long getLastModifiedTime() throws FileSystemException {
        try (HttpURLConnection connection = new HttpURLConnection(file.getURL())) {
            return connection.getLastModified();
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
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
        try (HttpURLConnection connection = new HttpURLConnection(file.getURL())) {
            return connection.getHeaderFields().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            (e) -> String.join(", ", e.getValue())));
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
            HttpURLConnection connection = new HttpURLConnection(file.getURL());
            return new ResourceInputStream(connection, connection.getInputStream());
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
        try (HttpURLConnection connection = new HttpURLConnection(file.getURL())) {
            return new DefaultFileContentInfo(connection.getContentType(), connection.getContentEncoding());
        } catch (IOException e) {
            throw new FileSystemException(e);
        }
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
            return input.transferTo(output, bufferSize);
        }
    }
}
