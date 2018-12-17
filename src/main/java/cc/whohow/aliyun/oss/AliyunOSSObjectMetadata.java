package cc.whohow.aliyun.oss;

import com.aliyun.oss.internal.OSSHeaders;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AliyunOSSObjectMetadata {
    public static final Set<String> RAW_META_DATA = Collections.unmodifiableSet(getRawMetaData());

    private static Set<String> getRawMetaData() {
        return Arrays.stream(OSSHeaders.class.getFields())
                .map(f -> {
                    try {
                        return (String) f.get(null);
                    } catch (IllegalAccessException e) {
                        throw new UndeclaredThrowableException(e);
                    }
                }).collect(Collectors.toSet());
    }

    public static String normalizeName(String name) {
        Objects.requireNonNull(name);
        name = name.toLowerCase();
        if (name.startsWith("x-oss-meta-")) {
            name = name.substring("x-oss-meta-".length());
        }
        return name;
    }
}
