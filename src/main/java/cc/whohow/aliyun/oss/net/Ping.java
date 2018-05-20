package cc.whohow.aliyun.oss.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * ping
 */
public class Ping {
    public static volatile int pingTimeout = 1000;

    public static long ping(String address) {
        return ping(address, pingTimeout);
    }

    public static long ping(String address, int timeout) {
        long timestamp = System.currentTimeMillis();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address, 80), timeout);
            return System.currentTimeMillis() - timestamp;
        } catch (IOException ignore) {
            return -1L;
        }
    }
}
