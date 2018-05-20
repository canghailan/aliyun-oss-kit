package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TestAliyunOSSFileSystem {
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

        AliyunOSS.configure(new AliyunOSSUri(accessKeyId, secretAccessKey, bucketName, endpoint, null));
        AliyunOSSVirtualFileSystem vfs = new AliyunOSSVirtualFileSystem();
        VFS.setManager(vfs);
    }

    @Test
    public void testVirtualFileSystem() throws Exception {
        FileObject temp = VFS.getManager().resolveFile("oss://yt-temp/temp/");
        for (FileObject file : temp) {
            System.out.println(file);
        }
    }

    @Test
    public void testFileName() throws Exception {
        System.out.println(new AliyunOSSFileName("oss://yt-temp/a/b/c"));
        Assert.assertEquals(
                "yt-temp",
                new AliyunOSSFileName("oss://yt-temp/a/b/c").getBucketName());
        Assert.assertEquals(
                "a/b/c",
                new AliyunOSSFileName("oss://yt-temp/a/b/c").getKey());
        Assert.assertEquals(
                "c",
                new AliyunOSSFileName("oss://yt-temp/a/b/c").getBaseName());
        Assert.assertEquals(
                "c",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getBaseName());
        Assert.assertEquals(
                "/a/b/c/",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getPath());
        Assert.assertEquals(
                "jpg",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getExtension());
        Assert.assertEquals(
                "",
                new AliyunOSSFileName("oss://yt-temp/.jpg").getExtension());
        Assert.assertEquals(
                "",
                new AliyunOSSFileName("oss://yt-temp/d").getExtension());
        Assert.assertEquals(
                "oss://yt-temp/a/b/c/d.jpg",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getFriendlyURI());
        Assert.assertEquals(
                "oss://yt-temp/",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getRootURI());
        Assert.assertEquals(
                "oss",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getScheme());
        Assert.assertEquals(
                new AliyunOSSFileName("yt-temp", ""),
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getRoot());
        Assert.assertEquals(
                new AliyunOSSFileName("yt-temp", "a/b/c"),
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getParent());
        Assert.assertNull(new AliyunOSSFileName("oss://yt-temp/").getParent());
        Assert.assertEquals(
                FileType.FILE,
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg").getType());
        Assert.assertEquals(
                FileType.FOLDER,
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getType());
        Assert.assertEquals(
                "oss://yt-temp/a/b/c/",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getURI());
        Assert.assertEquals(
                "d",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getRelativeName(
                        new AliyunOSSFileName("oss://yt-temp/a/b/c/d")));
        Assert.assertEquals(
                "d.jpg",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getRelativeName(
                        new AliyunOSSFileName("oss://yt-temp/a/b/c/d.jpg")));
        Assert.assertEquals(
                "../",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getRelativeName(
                        new AliyunOSSFileName("oss://yt-temp/a/b/")));
        Assert.assertEquals(
                "",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getRelativeName(
                        new AliyunOSSFileName("oss://yt-temp/a/b/c")));
        Assert.assertEquals(
                "../../../",
                new AliyunOSSFileName("oss://yt-temp/a/b/c/").getRelativeName(
                        new AliyunOSSFileName("oss://yt-temp/")));
        Assert.assertTrue(new AliyunOSSFileName("oss://yt-temp/a/b/c/").isAncestor(
                new AliyunOSSFileName("oss://yt-temp/")));
        Assert.assertFalse(new AliyunOSSFileName("oss://yt-temp/a/b/c/").isAncestor(
                new AliyunOSSFileName("oss://yt-temp/ad")));
        Assert.assertTrue(new AliyunOSSFileName("oss://yt-temp/a/b/c/").isDescendent(
                new AliyunOSSFileName("oss://yt-temp/a/b/c/d")));
        Assert.assertFalse(new AliyunOSSFileName("oss://yt-temp/a/b/c/").isDescendent(
                new AliyunOSSFileName("oss://yt-temp/ad")));
        Assert.assertTrue(new AliyunOSSFileName("oss://yt-temp/a/b/c/d").isFile());
        Assert.assertFalse(new AliyunOSSFileName("oss://yt-temp/a/b/c/").isFile());
    }

    @Test
    public void testJunctions() throws Exception {
        NavigableMap<String, String> junctions = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        junctions.put("oss://a1/","oss://a1/");
        junctions.put("oss://a1/b1/","oss://a1/b1/");
        junctions.put("oss://a1/b1/c1/","oss://a1/b1/c1/");
        junctions.put("oss://a1/b2/","oss://a1/b2/");
        junctions.put("oss://a1/b2/c2/","oss://a1/b2/c2/");
        junctions.put("oss://a1/b2/c2/d2/","oss://a1/b2/c2/d2/");
        junctions.put("oss://a1/b","oss://a1/b");

        junctions.keySet().forEach(System.out::println);

        System.out.println();
        junctions.tailMap("oss://a1/b2/c")
                .keySet().forEach(System.out::println);
    }
}
