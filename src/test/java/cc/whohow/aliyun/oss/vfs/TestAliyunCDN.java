package cc.whohow.aliyun.oss.vfs;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

public class TestAliyunCDN {
    @Test
    public void testSign() {
        URI uri = URI.create("https://dl.yitong.com/test.txt");
        String privateKey = "";
        long timestamp = System.currentTimeMillis() / 1000 + 1800;
        String rand = UUID.randomUUID().toString().replace("-", "");
        String uid = "0";
        String text = uri.getPath() + "-" + timestamp + "-" + rand + "-" + uid + "-" + privateKey;
        String md5 = DigestUtils.md5Hex(text);
        String url = uri.toASCIIString() + "?auth_key=" + timestamp + "-" + rand + "-" + uid + "-" + md5;

        System.out.println(text);
        System.out.println(md5);
        System.out.println(url);
    }
}
