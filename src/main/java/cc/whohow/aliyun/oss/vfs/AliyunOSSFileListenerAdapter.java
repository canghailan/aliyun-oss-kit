package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.AliyunOSSObjectAsync;
import cc.whohow.aliyun.oss.diff.DiffStatus;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.events.AbstractFileChangeEvent;
import org.apache.commons.vfs2.events.ChangedEvent;
import org.apache.commons.vfs2.events.CreateEvent;
import org.apache.commons.vfs2.events.DeleteEvent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

public class AliyunOSSFileListenerAdapter implements FileListener, BiConsumer<DiffStatus, AliyunOSSObjectAsync> {
    private AliyunOSSFileObject file;
    private List<FileListener> fileListeners = new CopyOnWriteArrayList<>();
    private ScheduledFuture<?> scheduledFuture;

    public AliyunOSSFileListenerAdapter(AliyunOSSFileObject file) {
        this.file = file;
    }

    public ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    public void addFileListener(FileListener fileListener) {
        this.fileListeners.add(fileListener);
    }

    public void removeFileListener(FileListener fileListener) {
        this.fileListeners.remove(fileListener);
    }

    public List<FileListener> getFileListeners() {
        return fileListeners;
    }

    @Override
    public void accept(DiffStatus type, AliyunOSSObjectAsync object) {
        try {
            AbstractFileChangeEvent event = newEvent(type, object);
            if (event != null) {
                event.notify(this);
            }
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    @Override
    public void fileCreated(FileChangeEvent event) {
        for (FileListener fileListener : fileListeners) {
            try {
                fileListener.fileCreated(event);
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void fileDeleted(FileChangeEvent event) {
        for (FileListener fileListener : fileListeners) {
            try {
                fileListener.fileDeleted(event);
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void fileChanged(FileChangeEvent event) {
        for (FileListener fileListener : fileListeners) {
            try {
                fileListener.fileChanged(event);
            } catch (Exception ignore) {
            }
        }
    }

    private AbstractFileChangeEvent newEvent(DiffStatus status, AliyunOSSObjectAsync object) {
        AliyunOSSFileObject fileObject = new AliyunOSSFileObject(file.getFileSystem(),
                new AliyunOSSFileName(object.getBucketName(), object.getKey()));
        switch (status) {
            case ADDED: {
                return new CreateEvent(fileObject);
            }
            case DELETED: {
                return new DeleteEvent(fileObject);
            }
            case MODIFIED: {
                return new ChangedEvent(fileObject);
            }
            default: {
                return null;
            }
        }
    }
}
