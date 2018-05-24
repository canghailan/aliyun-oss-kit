package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSS;
import cc.whohow.aliyun.oss.AliyunOSSUri;
import cc.whohow.aliyun.oss.configuration.AliyunOSSConfigurationManager;
import cc.whohow.configuration.FileBasedConfigurationManager;
import cc.whohow.configuration.provider.YamlConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;

public class TestAliyunOSSConfiguration {
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
        AliyunOSS.setExecutor(Executors.newScheduledThreadPool(1));
    }

    @AfterClass
    public static void tearDown() {
        AliyunOSS.shutdown();
    }

    @Test
    public void testConfiguration() throws Exception {
        FileBasedConfigurationManager configurationManager = new AliyunOSSConfigurationManager(
                AliyunOSS.getAliyunOSSObjectAsync("oss://yt-temp/test-kit/conf/"));

        try (YamlConfiguration configuration = new YamlConfiguration(configurationManager.get("constants.yml"))) {
            System.out.println(configuration.get());
        }

        new YamlConfiguration(configurationManager.get("constants.yml")).getAndWatch(System.out::println);

        Thread.sleep(180_1000L);
    }
}
