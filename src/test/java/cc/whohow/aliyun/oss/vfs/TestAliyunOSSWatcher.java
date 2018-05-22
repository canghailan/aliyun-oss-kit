package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import org.apache.commons.vfs2.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;

public class TestAliyunOSSWatcher {
    private static String accessKeyId = "";
    private static String secretAccessKey = "";
    private static String bucketName = "yt-temp";
    private static String endpoint = "oss-cn-hangzhou.aliyuncs.com";
    private static FileSystemManager vfs;

    @BeforeClass
    @SuppressWarnings("all")
    public static void setup() throws Exception {
        try (InputStream stream = new FileInputStream("oss.properties")) {
            Properties properties = new Properties();
            properties.load(stream);
            accessKeyId = properties.getProperty("accessKeyId");
            secretAccessKey = properties.getProperty("secretAccessKey");
        }

        AliyunOSS.configure(new AliyunOSSUri(accessKeyId, secretAccessKey, bucketName, endpoint, null));
        AliyunOSS.setExecutor(Executors.newScheduledThreadPool(5));

        vfs = new AliyunOSSVirtualFileSystem();
        VFS.setManager(vfs);
    }

    @AfterClass
    public static void tearDown() {
        AliyunOSS.shutdown();
    }

    @Test
    public void testWatchObjects() throws Exception {
        AliyunOSS.getAliyunOSSObjectAsync("oss://yt-temp/test-kit/").watch((status, object) -> {
            System.out.println(status + " " + object);
        });
        Thread.sleep(60_000L);
    }

    @Test
    public void testWatchObject() throws Exception {
        AliyunOSS.getAliyunOSSObjectAsync("oss://yt-temp/test-kit/.gitignore").watch((status, object) -> {
            System.out.println(status + " " + object);
        });
        Thread.sleep(180_000L);
    }

    @Test
    public void testWatchFileObject() throws Exception {
        FileObject fileObject = vfs.resolveFile("oss://yt-temp/test-fs/");
        fileObject.getFileSystem().addListener(fileObject, new FileListener() {
            @Override
            public void fileCreated(FileChangeEvent event) throws Exception {
                System.out.println("+ " + event.getFile());
            }

            @Override
            public void fileDeleted(FileChangeEvent event) throws Exception {
            }

            @Override
            public void fileChanged(FileChangeEvent event) throws Exception {
            }
        });
        fileObject.getFileSystem().addListener(fileObject, new FileListener() {
            @Override
            public void fileCreated(FileChangeEvent event) throws Exception {
            }

            @Override
            public void fileDeleted(FileChangeEvent event) throws Exception {
                System.out.println("- " + event.getFile());
            }

            @Override
            public void fileChanged(FileChangeEvent event) throws Exception {
            }
        });
        fileObject.getFileSystem().addListener(fileObject, new FileListener() {
            @Override
            public void fileCreated(FileChangeEvent event) throws Exception {
            }

            @Override
            public void fileDeleted(FileChangeEvent event) throws Exception {
            }

            @Override
            public void fileChanged(FileChangeEvent event) throws Exception {
                System.out.println("* " + event.getFile());
            }
        });
        Thread.sleep(180_000L);
    }
}
