package cc.whohow.aliyun.oss.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class Java9InputStream extends InputStream {
    protected final InputStream delegate;

    public Java9InputStream(InputStream delegate) {
        this.delegate = delegate;
    }

    public static long transfer(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        long transferred = 0L;
        while (true) {
            int n = input.read(buffer);
            if (n < 0) {
                return transferred;
            } else if (n > 0) {
                output.write(buffer, 0, n);
                transferred += n;
            }
        }
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    public byte[] readAllBytes() throws IOException {
        return readAllBytes(new byte[8 * 1024]);
    }

    public byte[] readAllBytes(byte[] buffer) throws IOException {
        int offset = 0;
        int length = buffer.length;
        while (true) {
            int n = read(buffer, offset, length);
            if (n < 0) {
                if (buffer.length == offset) {
                    return buffer;
                } else {
                    return Arrays.copyOf(buffer, offset);
                }
            } else {
                offset += n;
                length -= n;
                if (length == 0) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                    length = buffer.length - offset;
                }
            }
        }
    }

    public int readNBytes(byte[] buffer, int offset, int length) throws IOException {
        int bytes = 0;
        while (length > 0) {
            int n = read(buffer, offset, length);
            if (n < 0) {
                return bytes;
            }
            if (n > 0) {
                bytes += n;
                offset += n;
                length -= n;
            }
        }
        return bytes;
    }

    public long transferTo(OutputStream out) throws IOException {
        return transferTo(out, new byte[8 * 1024]);
    }

    public long transferTo(OutputStream out, byte[] buffer) throws IOException {
        return transfer(this, out, buffer);
    }
}
