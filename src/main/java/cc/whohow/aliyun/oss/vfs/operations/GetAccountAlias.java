package cc.whohow.aliyun.oss.vfs.operations;

import org.apache.commons.vfs2.operations.FileOperation;

import java.util.function.Supplier;

public interface GetAccountAlias extends FileOperation, Supplier<String> {
}
