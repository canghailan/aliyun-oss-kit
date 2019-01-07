package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.copy.FileObjectCopier;
import cc.whohow.aliyun.oss.vfs.operations.*;
import cc.whohow.vfs.FluentFileObject;
import cc.whohow.vfs.VirtualFileSystemManager;
import cc.whohow.vfs.configuration.JsonConfigurationFile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.vfs2.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

public class TestAliyunOSSFileSystem {
    private static VirtualFileSystemManager vfs;

    @BeforeClass
    @SuppressWarnings("all")
    public static void setup() throws Exception {
        try (InputStream stream = new FileInputStream("vfs.yml")) {
            JsonNode conf = new ObjectMapper(new YAMLFactory()).readTree(stream);
            vfs = new VirtualFileSystemManager(new JsonConfigurationFile(conf));
            VFS.setManager(vfs);
        }

        vfs.addJunction("/temp/", "oss://yt-temp/");
        vfs.addJunction("/temp/test-fs/", "oss://yt-temp/test-fs/");
    }

    @AfterClass
    public static void tearDown() throws FileSystemException {
        vfs.close();
    }

    @Test
    public void testVirtualFileSystem() throws Exception {
        FileObject temp = vfs.resolveFile("oss://yt-temp/test-fs/");
        System.out.println(temp.getPublicURIString());
        System.out.println(vfs.resolveFile("https://temp.yitong.com/test-fs/url/random.jpg").getClass());
        System.out.println(vfs.resolveFile("https://temp.yitong.com/test-fs/url/random.jpg@compressed.jpg").getClass());
    }

    @Test
    public void testCopyFrom() throws Exception {
        vfs.resolveFile("oss://yt-temp/test-fs/")
                .copyFrom(vfs.resolveFile("oss://yt-temp/test-kit/pom.xml"), Selectors.SELECT_ALL);
        vfs.resolveFile("oss://yt-temp/test-fs/sync/")
                .copyFrom(vfs.resolveFile("oss://yt-temp/test-kit/sync/"), Selectors.SELECT_ALL);
        vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg")
                .copyFrom(vfs.resolveFile("oss://yt-temp/test-kit/url/random.jpg"), Selectors.SELECT_ALL);
        vfs.resolveFile("oss://yt-temp/test-fs/url/random.jpg")
                .copyFrom(vfs.resolveFile("https://picsum.photos/200/300/?random"), Selectors.SELECT_ALL);
    }

    @Test
    public void testDelete() throws Exception {
        vfs.resolveFile("oss://yt-temp/test-fs/url/random.jpg").delete();
        vfs.resolveFile("oss://yt-temp/test-fs/sync/src/test/").deleteAll();
    }

    @Test
    public void testExists() throws Exception {
        Assert.assertFalse(vfs.resolveFile("oss://yt-temp/test-fs/url/random.jpg").exists());
        Assert.assertTrue(vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg").exists());
    }

    @Test
    public void testList() throws Exception {
        FluentFileObject.of(vfs.resolveFile("oss://yt-temp/test-fs/sync/src/main/java/cc/whohow/aliyun/oss/"))
                .list()
                .forEach(System.out::println);
    }

    @Test
    public void testListRecursively() throws Exception {
        FluentFileObject.of(vfs.resolveFile("oss://yt-temp/test-fs/sync/src/main/java/cc/whohow/aliyun/oss/"))
                .listRecursively()
                .forEach(System.out::println);
    }

    @Test
    public void testFind() throws Exception {
        FluentFileObject.of(vfs.resolveFile("oss://yt-temp/test-fs/sync/src/main/java/cc/whohow/aliyun/oss/"))
                .find()
                .forEach(System.out::println);
    }

    @Test
    public void testFindFile() throws Exception {
        FluentFileObject.of(vfs.resolveFile("oss://yt-temp/test-fs/sync/src/main/java/cc/whohow/aliyun/oss/"))
                .find(Selectors.SELECT_FILES)
                .forEach(System.out::println);
    }

    @Test
    public void testFindExcludeSelf() throws Exception {
        FluentFileObject.of(vfs.resolveFile("oss://yt-temp/test-fs/sync/src/main/java/cc/whohow/aliyun/oss/"))
                .find(Selectors.EXCLUDE_SELF)
                .forEach(System.out::println);
    }

    @Test
    public void testSelectFiles() throws Exception {
        System.out.println("SELECT_FILES");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_FILES)) {
            System.out.println(file);
        }
    }

    @Test
    public void testFindFiles() throws Exception {
        System.out.println("SELECT_SELF");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_SELF)) {
            System.out.println(file);
        }
        System.out.println("SELECT_SELF_AND_CHILDREN");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_SELF_AND_CHILDREN)) {
            System.out.println(file);
        }
        System.out.println("SELECT_CHILDREN");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_CHILDREN)) {
            System.out.println(file);
        }
        System.out.println("EXCLUDE_SELF");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.EXCLUDE_SELF)) {
            System.out.println(file);
        }
        System.out.println("SELECT_FILES");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_FILES)) {
            System.out.println(file);
        }
        System.out.println("SELECT_FOLDERS");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_FOLDERS)) {
            System.out.println(file);
        }
        System.out.println("SELECT_ALL");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(Selectors.SELECT_ALL)) {
            System.out.println(file);
        }
        System.out.println("FileDepthSelector(0, 2)");
        for (FileObject file : vfs.resolveFile("oss://yt-temp/test-kit/").findFiles(new FileDepthSelector(0, 2))) {
            System.out.println(file);
        }
    }

    @Test
    public void testGetChild() throws Exception {
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").getChild("pom.xml"));
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").getChild("copy/"));
    }

    @Test
    public void testGetChildren() throws Exception {
        for (FileObject fileObject : vfs.resolveFile("oss://yt-temp/test-fs/").getChildren()) {
            System.out.println(fileObject);
        }
    }

    @Test
    public void testGetContent() throws Exception {
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg")
                .getContent().getContentInfo().getContentType());
    }

    @Test
    public void testGetParent() throws Exception {
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg").getParent());
    }

    @Test
    public void testGetPublicURIString() throws Exception {
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg").getPublicURIString());
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/pom.xml").getPublicURIString());
    }

    @Test
    public void testGetType() throws Exception {
        Assert.assertEquals(FileType.FILE, vfs.resolveFile("oss://yt-temp/test-fs/copy/random.jpg").getType());
        Assert.assertEquals(FileType.FOLDER, vfs.resolveFile("oss://yt-temp/test-fs/copy/").getType());
    }

    @Test
    public void testResolveFile() throws Exception {
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").resolveFile("copy/"));
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").resolveFile("pom.xml"));
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").resolveFile("sync/src/main/"));
        System.out.println(vfs.resolveFile("oss://yt-temp/test-fs/").resolveFile("sync/src/main/java/cc/whohow/aliyun/oss/AliyunOSS.java"));
    }

    @Test
    public void testImageCompress() throws Exception {
//        FileObject file = vfs.resolveFile("oss://yt-temp/temp/0025802e-11d5-4eb8-879e-c04cc2588edd.jpg");
//        System.out.println(file);
//        System.out.println(file.getName().getBaseName());
//        System.out.println(file.getName().getExtension());
//        System.out.println(file.getName().getPath());
//        System.out.println(file.getName().getParent());
//        System.out.println(file.getName().getType());
//        System.out.println(file.getPublicURIString());
//        System.out.println(FluentFileObject.of(file)
//                .doOperation(ProcessImage.class, (f, op) -> op.setParameters("@compress.jpg").get()));
//        vfs.resolveFile("oss://yt-temp/test-kit/a.jpg")
//                .copyFrom(FileObjects.newOperation(file, ProcessImage.class).setParameters("@compress.jpg").get(), Selectors.SELECT_ALL);
    }

    @Test
    public void testCompareFileContent() throws Exception {
//        FileObject fileObject1 = vfs.resolveFile("oss://yt-temp/test-kit/pom.xml");
//        FileObject fileObject2 = vfs.resolveFile("oss://yt-temp/test-kit/a.jpg");
//        FileObject fileObject3 = vfs.resolveFile("oss://yt-temp/test-fs/pom.xml");
//        Assert.assertTrue(FileObjects.newOperation(fileObject1, CompareFileContent.class).setFileObjectForCompare(fileObject3).isIdentical());
//        Assert.assertTrue(FileObjects.newOperation(fileObject1, CompareFileContent.class).setFileObjectForCompare(fileObject2).isDifferent());
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
                new AliyunOSSFileName("yt-temp", "a/b/c/"),
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
    public void testGetUriFileAttributes() throws Exception {
        FileObject file = vfs.resolveFile("https://picsum.photos/200/300/?random");
        System.out.println(file.getContent().getAttributes());
    }

    @Test
    public void testGeneratePresignedUrl() throws Exception {
//        String signedUrl = FileObjects.newOperation(vfs.resolveFile("oss://yt-temp/test-kit/copy/random.jpg"), GetSignedUrl.class)
//                .setExpiresIn(Duration.ofSeconds(3L * 60L * 60L * 1000L))
//                .get();
//        System.out.println(signedUrl);
    }

    @Test
    public void testJunctions() throws Exception {
        NavigableMap<String, String> junctions = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        junctions.put("oss://a1/", "oss://a1/");
        junctions.put("oss://a1/b1/", "oss://a1/b1/");
        junctions.put("oss://a1/b1/c1/", "oss://a1/b1/c1/");
        junctions.put("oss://a1/b2/", "oss://a1/b2/");
        junctions.put("oss://a1/b2/c2/", "oss://a1/b2/c2/");
        junctions.put("oss://a1/b2/c2/d2/", "oss://a1/b2/c2/d2/");
        junctions.put("oss://a1/b", "oss://a1/b");

        junctions.keySet().forEach(System.out::println);

        System.out.println();
        junctions.tailMap("oss://a1/b2/c")
                .keySet().forEach(System.out::println);
    }

    @Test
    public void testSize() throws Exception {
//        System.out.println(FileObjects.size(vfs.resolveFile("oss://yt-temp/test-kit/copy/")));
    }

    @Test
    public void testFileObjectCopier() throws Exception {
        FileObject file = new FileObjectCopier()
                .setSource("oss://yt-temp/test-kit/copy/d6b067fc-8854-44b1-9694-8c9564ec836b.jpg")
                .setTarget("oss://yt-temp/test-kit/copy/")
                .withRandomTarget()
                .withSourceOperation(ProcessImage.class, (s, op) -> op.setParameters("@!thumb.jpg").get())
                .call();
        System.out.println(file);
    }


    @Test
    public void testSyncFromFile() throws Throwable {
//        FileObject file = VFS.getManager().toFileObject(new File("src"));
//        new FileSynchronizer(file, )
//        new DiffFormatter().print(AliyunOSS.getAliyunOSSObject("oss://yt-temp/test-kit/sync/src/")
//                .syncFromFile(new File("src")));
    }

    //
//    @Test
//    public void testConfigurationManager() throws Exception {
//        FileBasedConfigurationManager configurationManager = new VfsConfigurationManager(
//                AliyunOSS.getAliyunOSSObjectAsync("oss://yt-temp/test-kit/conf/"));
//
//        try (YamlConfiguration<JsonNode> configuration = new YamlConfiguration<>(configurationManager.get("constants.yml"), JsonNode.class)) {
//            System.out.println(configuration.get());
//        }
//
//        new YamlConfiguration<>(configurationManager.get("constants.yml"), JsonNode.class).getAndWatch(System.out::println);
//
//        Thread.sleep(180_1000L);
//    }
//
//    @Test
//    public void testConfiguration() throws Exception {
//        try (PropertiesConfiguration configuration = new PropertiesConfiguration(new AliyunOSSConfigurationSource(
//                AliyunOSS.getAliyunOSSObject("")))) {
//            System.out.println(configuration.get());
//        }
//    }

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

    @Test
    public void testAccountAlias() throws Exception {
        GetAccountAlias getAccountAlias = (GetAccountAlias)
                vfs.resolveFile("oss://yt-temp/test-fs/url/random.jpg")
                .getFileOperations().getOperation(GetAccountAlias.class);
        System.out.println(getAccountAlias.get());
    }
}
