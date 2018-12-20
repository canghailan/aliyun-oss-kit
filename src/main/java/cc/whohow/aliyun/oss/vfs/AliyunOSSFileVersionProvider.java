package cc.whohow.aliyun.oss.vfs;

import cc.whohow.vfs.FluentFileObject;
import cc.whohow.vfs.version.FileAttributeVersionProvider;
import cc.whohow.vfs.version.FileVersion;
import com.aliyun.oss.internal.OSSHeaders;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;

import java.io.UncheckedIOException;
import java.util.stream.Stream;

public class AliyunOSSFileVersionProvider extends FileAttributeVersionProvider {
    public AliyunOSSFileVersionProvider() {
        super(OSSHeaders.ETAG);
    }

    @Override
    public FileVersion<Object> getVersion(FileObject fileObject) {
        try {
            if (fileObject.isFile()) {
                return super.getVersion(fileObject);
            } else {
                return new FileVersion<>(fileObject, null);
            }
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<FileVersion<Object>> getVersions(FileObject fileObject) {
        try {
            if (fileObject.isFile()) {
                return Stream.of(getVersion(fileObject));
            } else {
                return FluentFileObject.of(fileObject)
                        .find(Selectors.SELECT_FILES)
                        .map(this::getVersion);
            }
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }
}
