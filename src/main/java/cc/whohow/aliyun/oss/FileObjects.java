package cc.whohow.aliyun.oss;

import cc.whohow.aliyun.oss.io.*;
import cc.whohow.aliyun.oss.vfs.StreamFileObjectAdapter;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.operations.FileOperation;

import java.io.*;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * FileObjects 工具
 *
 * @see java.nio.file.Files
 */
public class FileObjects {
    /**
     * 获取文件对象
     */
    public static FileObject resolve(String uri) {
        try {
            return VFS.getManager().resolveFile(uri);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取输出流
     */
    public static InputStream newInputStream(FileObject fileObject) throws IOException {
        FileContent fileContent = fileObject.getContent();
        return new ResourceInputStream(fileContent, fileContent.getInputStream());
    }

    /**
     * 获取输入流
     */
    public static OutputStream newOutputStream(FileObject fileObject) throws IOException {
        FileContent fileContent = fileObject.getContent();
        return new ResourceOutputStream(fileContent, fileContent.getOutputStream());
    }

    /**
     * 获取文件后缀，若无，返回空字符串
     */
    public static String getSuffix(FileObject fileObject) {
        return getSuffix(fileObject.getName());
    }

    /**
     * 获取文件后缀，若无，返回空字符串
     */
    public static String getSuffix(FileName fileName) {
        String extension = fileName.getExtension();
        if (extension == null || extension.isEmpty()) {
            return "";
        }
        return "." + extension;
    }

    /**
     * 创建临时文件
     */
    public static FileObject createTempFile(FileObject dir, String suffix) {
        return createTempFile(dir, null, suffix);
    }

    /**
     * 创建临时文件
     */
    public static FileObject createTempFile(FileObject dir, String prefix, String suffix) {
        try {
            FileObject temp = dir.getChild(newRandomPath(prefix, suffix));
            temp.createFile();
            return temp;
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 生成随机路径
     */
    public static String newRandomPath(String prefix, String suffix) {
        if (prefix == null) {
            prefix = "";
        }
        if (suffix == null) {
            suffix = "";
        }
        return prefix + UUID.randomUUID() + suffix;
    }

    /**
     * 删除文件
     */
    public static void delete(FileObject fileObject) {
        try {
            fileObject.deleteAll();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 删除文件，忽略所有错误
     */
    public static void deleteQuietly(FileObject fileObject) {
        try {
            fileObject.deleteAll();
        } catch (Exception ignore) {
        }
    }

    /**
     * 拷贝文件
     */
    public static void copy(FileObject source, FileObject target) {
        try {
            target.copyFrom(source, Selectors.SELECT_ALL);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 移动文件
     */
    public static void move(FileObject source, FileObject target) {
        try {
            source.moveTo(target);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 猜测文件类型
     */
    public static String guessContentType(FileObject fileObject) {
        return guessContentType(fileObject.getName());
    }

    /**
     * 猜测文件类型
     */
    public static String guessContentType(FileName fileName) {
        return URLConnection.guessContentTypeFromName(fileName.getBaseName());
    }

    /**
     * 读取文件属性
     */
    public static Map<String, Object> readAttributes(FileObject fileObject) {
        try (FileContent fileContent = fileObject.getContent()) {
            return fileContent.getAttributes();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 是否是目录
     */
    public static boolean isDirectory(FileObject fileObject) {
        try {
            return fileObject.isFolder();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 是否是文件
     */
    public static boolean isRegularFile(FileObject fileObject) {
        try {
            return fileObject.isFile();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取文件最后修改时间
     */
    public static long getLastModifiedTime(FileObject fileObject) {
        try (FileContent fileContent = fileObject.getContent()) {
            return fileContent.getLastModifiedTime();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取文件大小
     */
    public static long size(FileObject fileObject) {
        try {
            if (fileObject.isFolder()) {
                try (Stream<FileObject> stream = walk(fileObject)) {
                    return stream
                            .filter(FileObjects::isRegularFile)
                            .mapToLong(FileObjects::size)
                            .sum();
                }
            } else {
                try (FileContent fileContent = fileObject.getContent()) {
                    return fileContent.getSize();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 文件是否存在
     */
    public static boolean exists(FileObject fileObject) {
        try {
            return fileObject.exists();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 文件是否不存在
     */
    public static boolean notExists(FileObject fileObject) {
        return !exists(fileObject);
    }

    /**
     * 拷贝流到文件
     */
    public static void copy(InputStream source, FileObject target) throws IOException {
        copy(new StreamFileObjectAdapter(source), target);
    }

    /**
     * 拷贝文件到流
     */
    public static void copy(FileObject source, OutputStream target) throws IOException {
        try (FileContent fileContent = source.getContent()) {
            try (Java9InputStream stream = new Java9InputStream(fileContent.getInputStream())) {
                stream.transferTo(target);
            }
        }
    }

    /**
     * 读取所有字节
     */
    public static byte[] readAllBytes(FileObject fileObject) {
        try (FileContent fileContent = fileObject.getContent()) {
            try (Java9InputStream stream = new Java9InputStream(fileContent.getInputStream())) {
                return stream.readAllBytes();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 读取所有字节到缓冲区
     */
    public static ByteBuffer read(FileObject fileObject) {
        try (FileContent fileContent = fileObject.getContent()) {
            try (Java9InputStream stream = new Java9InputStream(fileContent.getInputStream())) {
                return stream.readAllBytes(8 * 1024);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写入文件
     */
    public static void write(FileObject fileObject, byte[] bytes) {
        try {
            copy(new ByteArrayInputStream(bytes), fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写入文件
     */
    public static void write(FileObject fileObject, ByteBuffer bytes) {
        try {
            copy(new ByteBufferInputStream(bytes), fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 读取字符串
     */
    public static String read(FileObject fileObject, Charset charset) {
        return charset.decode(read(fileObject)).toString();
    }

    /**
     * 写入字符串
     */
    public static void write(FileObject fileObject, CharSequence text, Charset charset) {
        write(fileObject, charset.encode(CharBuffer.wrap(text)));
    }

    /**
     * 读取utf-8字符串
     */
    public static String readUtf8(FileObject fileObject) {
        return read(fileObject, StandardCharsets.UTF_8);
    }

    /**
     * 写入utf-8字符串
     */
    public static void writeUtf8(FileObject fileObject, CharSequence text) {
        write(fileObject, text, StandardCharsets.UTF_8);
    }

    /**
     * 列出文件夹下所有文件
     */
    public static Stream<FileObject> list(FileObject fileObject) throws IOException {
        return Arrays.stream(fileObject.getChildren());
    }

    /**
     * 递归列出文件夹下所有文件
     */
    public static Stream<FileObject> walk(FileObject fileObject) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(fileObject.iterator(), 0), false);
    }

    /**
     * 按行读取文件
     */
    public static Stream<String> lines(FileObject fileObject, Charset charset) {
        BufferedReader reader = null;
        try {
            try {
                reader = new BufferedReader(new InputStreamReader(newInputStream(fileObject), charset));
                return reader.lines().onClose(new UncheckedRunnable(reader));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } catch (RuntimeException e) {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException ignore) {
            }
            throw e;
        }
    }

    /**
     * 获取文件扩展操作
     */
    @SuppressWarnings("unchecked")
    public static <OP extends FileOperation> OP newOperation(FileObject fileObject, Class<OP> fileOperation) {
        try {
            return (OP) fileObject.getFileOperations().getOperation(fileOperation);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }
}
