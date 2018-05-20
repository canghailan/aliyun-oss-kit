package cc.whohow.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.AppendObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OSS写入流，通过appendObject实现
 */
public class AliyunOSSOutputStream extends OutputStream {
    private final OSS oss;
    private final String bucketName;
    private final String key;
    private long position;

    public AliyunOSSOutputStream(OSS oss, String bucketName, String key) {
        this(oss, bucketName, key, 0L);
    }

    public AliyunOSSOutputStream(OSS oss, String bucketName, String key, long position) {
        this.oss = oss;
        this.bucketName = bucketName;
        this.key = key;
        this.position = position;
    }

    public OSS getOSS() {
        return oss;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b});
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        position = oss.appendObject(
                new AppendObjectRequest(bucketName, key, new ByteArrayInputStream(b, off, len))
                        .withPosition(position)).getNextPosition();
    }
}
