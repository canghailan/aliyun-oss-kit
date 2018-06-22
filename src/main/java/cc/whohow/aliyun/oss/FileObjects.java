package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.vfs.operations.CompareFileContent;
import cc.whohow.aliyun.oss.vfs.operations.GetSignedUrl;
import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;

import java.io.UncheckedIOException;
import java.time.Duration;

public class FileObjects {
    public static void copy(FileObject source, FileObject target) {
        try {
            target.copyFrom(source, Selectors.SELECT_ALL);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isFileContentIdentical(FileObject a, FileObject b) {
        return CompareFileContent.apply(a).setFileObjectForCompare(b).isIdentical();
    }

    public static boolean isFileContentDifferent(FileObject a, FileObject b) {
        return CompareFileContent.apply(a).setFileObjectForCompare(b).isDifferent();
    }

    public static String getSignedUrl(FileObject fileObject, Duration expiresIn) {
        return GetSignedUrl.apply(fileObject).setExpiresIn(expiresIn).get();
    }

    public static FileObject processImage(FileObject fileObject, String parameters) {
        return ProcessImage.apply(fileObject).setParameters(parameters).get();
    }
}
