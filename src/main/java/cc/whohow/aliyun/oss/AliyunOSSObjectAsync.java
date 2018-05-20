package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.diff.DiffStatus;
import com.aliyun.oss.OSS;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class AliyunOSSObjectAsync extends AliyunOSSObject {
    protected ScheduledExecutorService executor;

    public AliyunOSSObjectAsync(OSS oss, String bucketName, String key, ScheduledExecutorService executor) {
        super(oss, bucketName, key);
        this.executor = executor;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public ScheduledFuture<?> watch(BiConsumer<DiffStatus, ? super AliyunOSSObjectAsync> listener) {
        return watch(Duration.ofSeconds(1L), listener);
    }

    public ScheduledFuture<?> watch(Duration interval, BiConsumer<DiffStatus, ? super AliyunOSSObjectAsync> listener) {
        return executor.scheduleWithFixedDelay(new AliyunOSSObjectWatchTask(this, listener), 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }
}
