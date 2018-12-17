package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.*;
import cc.whohow.vfs.synchronize.FileSynchronizer;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.provider.local.LocalFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestAliyunOSS {
    private static String accessKeyId = "";
    private static String secretAccessKey = "";
    private static String bucketName = "yt-temp";
    private static String endpoint = "oss-cn-hangzhou.aliyuncs.com";

    @BeforeClass
    @SuppressWarnings("all")
    public static void setup() throws Exception {
        try (InputStream stream = new FileInputStream("oss.properties")) {
            Properties properties = new Properties();
            properties.load(stream);
            accessKeyId = properties.getProperty("accessKeyId");
            secretAccessKey = properties.getProperty("secretAccessKey");
        }

        AliyunOSS.configure(accessKeyId, secretAccessKey);
        AliyunOSS.setExecutor(Executors.newScheduledThreadPool(8));
    }

    @AfterClass
    public static void tearDown() {
        AliyunOSS.shutdown();
    }

    @Test
    public void testAliyunOSSContext() {
        long t = System.currentTimeMillis();
        try (AliyunOSSContext context = new AliyunOSSContext()) {
            context.addProfile(DefaultProfile.getProfile(null, accessKeyId, secretAccessKey));
            System.out.println(context);
        }
        System.out.println((System.currentTimeMillis() - t) + "ms");
    }

    @Test
    public void testListObjectSummaries() {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/temp/").listObjectSummaries().forEachRemaining(o -> {
            System.out.println(o.getETag() + " oss://" + o.getBucketName() + "/" + o.getKey());
        });
    }

    @Test
    public void testListObjectSummariesRecursively() {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/temp/").listObjectSummariesRecursively().forEachRemaining(o -> {
            System.out.println(o.getETag() + " oss://" + o.getBucketName() + "/" + o.getKey());
        });
    }

    @Test
    public void testPutObjectStream() throws Exception {
        try (InputStream stream = new FileInputStream(new File("pom.xml"))) {
            AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/stream/pom.xml").putObject(stream);
        }
    }

    @Test
    public void testPutObjectFile() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml")
                .putObject(new File("pom.xml"));
    }

    @Test
    public void testPutObjectUrl() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/random.jpg")
                .putObject(new URL("https://picsum.photos/200/300/?random"));
    }

    @Test
    public void testPutLargeObjectUrl() throws Exception {
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tasks.add(() -> {
                long timestamp = System.currentTimeMillis();
                System.out.println(Thread.currentThread().getId());
                try {
                    AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/random" + Thread.currentThread().getId() + ".jpg")
                            .putObject(new URL("https://picsum.photos/200/300/?random"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getId() + ": " + (System.currentTimeMillis() - timestamp));
            });
        }
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);
        tasks.forEach(executor::submit);
//        executor.scheduleWithFixedDelay(() -> {
//            HttpClient httpClient = HttpURLConnection.httpClient;
//        }, 1,1, TimeUnit.SECONDS);
        executor.awaitTermination(1, TimeUnit.DAYS);
        executor.shutdownNow();
    }

    @Test
    public void testPutObjectObject() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/random1.jpg")
                .putObject(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/random.jpg"));
    }

    @Test
    public void testPutObjectRecursively() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/src/")
                .putObjectRecursively(new File("src"));
    }

    @Test
    public void testPutObjectObjectRecursively() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url2/")
                .putObjectRecursively(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/"));
    }

    @Test
    public void testCopyFromObject() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/random.jpg")
                .copyFromObject(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/url/random.jpg"));
    }

    @Test
    public void testCopyFromObjectRecursively() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/src/")
                .copyFromObjectRecursively(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/src/"));
    }

    @Test
    public void testGetObject() throws Exception {
        try (OSSObject object = AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObject()) {
            System.out.println(IOUtils.readStreamAsString(object.getObjectContent(), "utf-8"));
        }
    }

    @Test
    public void testGetObjectFile() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObject(new File("target/pom.xml")));
    }

    @Test
    public void testGetObjectRecursively() throws Exception {
        System.out.println(new File("temp").getAbsolutePath());
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/").getObjectRecursively(new File("temp")));
    }

    @Test
    public void testGetObjectContent() throws Exception {
        try (InputStream stream = AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObjectContent()) {
            System.out.println(IOUtils.readStreamAsString(stream, "utf-8"));
        }
    }

    @Test
    public void testGetSimplifiedObjectMeta() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getSimplifiedObjectMeta());
    }

    @Test
    public void testGetObjectMetadata() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObjectMetadata().getRawMetadata());
    }

    @Test
    public void testSetObjectMetadata() throws Exception {
        ObjectMetadata objectMetadata = AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObjectMetadata();
        System.out.println(objectMetadata.getUserMetadata());
        objectMetadata.addUserMetadata("TestSetObjectMetadata".toLowerCase(), String.valueOf(System.currentTimeMillis()));
        System.out.println(objectMetadata.getUserMetadata());
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").setObjectMetadata(objectMetadata);
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/pom.xml").getObjectMetadata().getUserMetadata());
    }

    @Test
    public void testAppendObject() throws Exception {
        try (InputStream input = new FileInputStream("pom.xml");
             AliyunOSSOutputStream stream = AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/append/pom.xml").appendObject()) {
            byte[] buffer = new byte[128];
            while (true) {
                int n = input.read(buffer);
                if (n < 0) {
                    break;
                }
                System.out.println("read " + n);
                System.out.println(stream.getPosition());
                stream.write(buffer, 0, n);
                System.out.println(stream.getPosition());
            }
        }
    }

    @Test
    public void testDeleteObject() throws Exception {
        AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/file/").listObjectSummariesRecursively().forEachRemaining(o -> {
            if (o.getKey().endsWith(".xml")) {
                System.out.println(o.getKey());
                AliyunOSS.getAliyunOSSObject(new AliyunOSSUri(
                        null, null, o.getBucketName(), null, o.getKey())).deleteObject();
            }
        });
    }

    @Test
    public void testDeleteObjectRecursively() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/src/").deleteObjectsRecursively());
    }

    @Test
    public void testDoesObjectExist() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/random.jpg").doesObjectExist());
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/not-exists.jpg").doesObjectExist());
    }

    @Test
    public void testGeneratePresignedUrl() throws Exception {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/copy/random.jpg")
                .generatePresignedUrl(new Date(System.currentTimeMillis() + 3L * 60L * 60L * 1000L)));
    }

    @Test
    public void testUploadFile() throws Throwable {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/upload/a.mp4")
                .uploadFileRecursively(new File("D:\\test\\a.mp4").getAbsolutePath()));
    }

    @Test
    public void testUploadFileRecursively() throws Throwable {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/upload/target/")
                .uploadFileRecursively(new File("target").getAbsolutePath()));
    }

    @Test
    public void testDownloadFile() throws Throwable {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/upload/a.mp4")
                .downloadFile(new File("a.mp4").getAbsolutePath()));
    }

    @Test
    public void testDownloadFileRecursively() throws Throwable {
        System.out.println(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/upload/target/")
                .downloadFileRecursively(new File("temp").getAbsolutePath()));
    }

    @Test
    public void testRawMetadata() throws Throwable {
        System.out.println(AliyunOSSObjectMetadata.RAW_META_DATA);
    }
}
