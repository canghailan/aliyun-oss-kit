package cc.whohow.aliyun.oss.configuration;

import cc.whohow.aliyun.oss.AliyunOSSObject;
import cc.whohow.aliyun.oss.AliyunOSSObjectAsync;
import cc.whohow.aliyun.oss.diff.DiffStatus;
import cc.whohow.configuration.FileBasedConfigurationManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

public class AliyunOSSConfigurationManager implements FileBasedConfigurationManager, BiConsumer<DiffStatus, AliyunOSSObjectAsync> {
    private final AliyunOSSObjectAsync conf;
    private final ScheduledFuture<?> scheduledFuture;
    private final Map<String, AliyunOSSConfigurationSource> configurationSources = new ConcurrentHashMap<>();

    public AliyunOSSConfigurationManager(AliyunOSSObjectAsync conf) {
        this.conf = conf;
        this.scheduledFuture = conf.watch(this);
    }

    @Override
    public AliyunOSSConfigurationSource get(String key) {
        if (key.startsWith("/") || key.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        return configurationSources.computeIfAbsent(key, this::newConfigurationSource);
    }

    protected AliyunOSSConfigurationSource newConfigurationSource(String key) {
        return new AliyunOSSConfigurationSource(
                new AliyunOSSObject(conf.getOSS(), conf.getBucketName(), conf.getKey() + key));
    }

    @Override
    public void close() throws IOException {
        scheduledFuture.cancel(true);
    }

    @Override
    public void accept(DiffStatus diffStatus, AliyunOSSObjectAsync object) {
        if (diffStatus == DiffStatus.ADDED || diffStatus == DiffStatus.MODIFIED) {
            String configurationKey = object.getKey().substring(conf.getKey().length());
            get(configurationKey).reload();
        }
    }
}
