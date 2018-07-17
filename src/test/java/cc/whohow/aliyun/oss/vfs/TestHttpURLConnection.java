package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.net.HttpURLConnection;
import com.aliyun.oss.common.utils.IOUtils;
import org.junit.Test;

public class TestHttpURLConnection {
    @Test
    public void test() throws Exception {
        try (HttpURLConnection connection = new HttpURLConnection("https://yitong.com")) {
            System.out.println(IOUtils.readStreamAsString(connection.getInputStream(), "utf-8"));
        }
    }
}
