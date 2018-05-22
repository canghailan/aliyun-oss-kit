package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.diff.DiffStatus;

import java.util.Map;
import java.util.function.BiConsumer;

public class AliyunOSSObjectWatchTask implements Runnable {
    private final AliyunOSSObjectAsync object;
    private final BiConsumer<DiffStatus, ? super AliyunOSSObjectAsync> listener;
    private final AliyunOSSWatcher diff;

    public AliyunOSSObjectWatchTask(AliyunOSSObjectAsync object,
                                    BiConsumer<DiffStatus, ? super AliyunOSSObjectAsync> listener) {
        this.object = object;
        this.listener = listener;
        this.diff = new AliyunOSSWatcher(object);
    }

    private AliyunOSSObjectAsync resolve(String key) {
        return new AliyunOSSObjectAsync(object.getOSS(), object.getBucketName(), key, object.getExecutor());
    }

    @Override
    public void run() {
        for (Map.Entry<String, DiffStatus> diff : diff.call().entrySet()) {
            if (diff.getValue() != DiffStatus.NOT_MODIFIED) {
                listener.accept(diff.getValue(), resolve(diff.getKey()));
            }
        }
    }
}
