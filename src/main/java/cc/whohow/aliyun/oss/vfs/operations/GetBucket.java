package cc.whohow.aliyun.oss.vfs.operations;

import com.aliyun.oss.model.Bucket;
import org.apache.commons.vfs2.operations.FileOperation;

import java.util.function.Supplier;

public interface GetBucket extends FileOperation, Supplier<Bucket>  {
}
