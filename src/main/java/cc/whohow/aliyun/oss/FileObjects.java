package cc.whohow.aliyun.oss;

import cc.whohow.vfs.io.*;
import cc.whohow.vfs.provider.stream.StreamFileObject;
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
 * @see cc.whohow.vfs.FluentFileObject
 */
@Deprecated
public class FileObjects {
    /**
     * 获取文件对象
     */
    public static FileObject get(String uri) {
        try {
            return VFS.getManager().resolveFile(uri);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取文件公开链接
     */
    public static String getPublicURIString(String uri) {
        try (FileObject fileObject = get(uri)) {
            return fileObject.getPublicURIString();
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 获取输出流
     */
    public static InputStream newInputStream(String uri) throws IOException {
        FileObject fileObject = get(uri);
        FileContent fileContent = fileObject.getContent();
        return new ResourceInputStream(fileContent, new ResourceInputStream(fileContent, fileContent.getInputStream()));
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
    public static OutputStream newOutputStream(String uri) throws IOException {
        FileObject fileObject = get(uri);
        FileContent fileContent = fileObject.getContent();
        return new ResourceOutputStream(fileObject, new ResourceOutputStream(fileContent, fileContent.getOutputStream()));
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
    public static String getSuffix(String uri) {
        try (FileObject fileObject = get(uri)) {
            return getSuffix(fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static FileObject createTempFile(String dir, String suffix) {
        try (FileObject fileObject = get(dir)) {
            return createTempFile(fileObject, suffix);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static FileObject createTempFile(String dir, String prefix, String suffix) {
        try (FileObject fileObject = get(dir)) {
            return createTempFile(fileObject, prefix, suffix);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static void delete(String uri) {
        try (FileObject fileObject = get(uri)) {
            delete(fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static void deleteQuietly(String uri) {
        try (FileObject fileObject = get(uri)) {
            deleteQuietly(fileObject);
        } catch (IOException ignore) {
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
    public static void copy(String source, String target) {
        try (FileObject sourceFileObject = get(source);
             FileObject targetFileObject = get(target)) {
            copy(sourceFileObject, targetFileObject);
        } catch (FileSystemException e) {
            throw new UncheckedIOException(e);
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
    public static void move(String source, String target) {
        try (FileObject sourceFileObject = get(source);
             FileObject targetFileObject = get(target)) {
            copy(sourceFileObject, targetFileObject);
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
    public static String guessContentType(String uri) {
        try (FileObject fileObject = get(uri)) {
            return guessContentType(fileObject);
        } catch (IOException e) {
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
    public static Map<String, Object> readAttributes(String uri) {
        try (FileObject fileObject = get(uri)) {
            return readAttributes(fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static boolean isDirectory(String uri) {
        try (FileObject fileObject = get(uri)) {
            return isDirectory(fileObject);
        } catch (IOException e) {
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
    public static boolean isRegularFile(String uri) {
        try (FileObject fileObject = get(uri)) {
            return isRegularFile(fileObject);
        } catch (IOException e) {
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
    public static long getLastModifiedTime(String uri) {
        try (FileObject fileObject = get(uri)) {
            return getLastModifiedTime(fileObject);
        } catch (IOException e) {
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
    public static long size(String uri) {
        try (FileObject fileObject = get(uri)) {
            return size(fileObject);
        } catch (IOException e) {
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
    public static boolean exists(String uri) {
        try (FileObject fileObject = get(uri)) {
            return exists(fileObject);
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
    public static boolean notExists(String uri) {
        try (FileObject fileObject = get(uri)) {
            return notExists(fileObject);
        } catch (IOException e) {
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
    public static void copy(InputStream source, String target) {
        try (FileObject fileObject = get(target)) {
            copy(source, fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 拷贝流到文件
     */
    public static void copy(InputStream source, FileObject target) {
        copy(new StreamFileObject(source), target);
    }

    /**
     * 拷贝文件到流
     */
    public static void copy(String source, OutputStream target) {
        try (FileObject fileObject = get(source)) {
            copy(fileObject, target);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 拷贝文件到流
     */
    public static void copy(FileObject source, OutputStream target) {
        try (FileContent fileContent = source.getContent()) {
            try (Java9InputStream stream = new Java9InputStream(fileContent.getInputStream())) {
                stream.transferTo(target);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 读取所有字节
     */
    public static byte[] readAllBytes(String uri) {
        try (FileObject fileObject = get(uri)) {
            return readAllBytes(fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public static ByteBuffer read(String uri) {
        try (FileObject fileObject = get(uri)) {
            return read(fileObject);
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
    public static void write(String uri, byte[] bytes) {
        try (FileObject fileObject = get(uri)) {
            write(fileObject, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写入文件
     */
    public static void write(FileObject fileObject, byte[] bytes) {
        copy(new ByteArrayInputStream(bytes), fileObject);
    }

    /**
     * 写入文件
     */
    public static void write(String uri, ByteBuffer bytes) {
        try (FileObject fileObject = get(uri)) {
            write(fileObject, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写入文件
     */
    public static void write(FileObject fileObject, ByteBuffer bytes) {
        copy(new ByteBufferInputStream(bytes), fileObject);
    }

    /**
     * 读取字符串
     */
    public static String read(String uri, Charset charset) {
        try (FileObject fileObject = get(uri)) {
            return read(fileObject, charset);
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
    public static void write(String uri, CharSequence text, Charset charset) {
        try (FileObject fileObject = get(uri)) {
            write(fileObject, text, charset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static String readUtf8(String uri) {
        try (FileObject fileObject = get(uri)) {
            return readUtf8(fileObject);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static void writeUtf8(String uri, CharSequence text) {
        try (FileObject fileObject = get(uri)) {
            writeUtf8(fileObject, text);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public static Stream<FileObject> list(String uri) throws IOException {
        FileObject fileObject = get(uri);
        return list(fileObject).onClose(new UncheckedRunnable(fileObject));
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
    public static Stream<FileObject> walk(String uri) throws IOException {
        FileObject fileObject = get(uri);
        return walk(fileObject).onClose(new UncheckedRunnable(fileObject));
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
    public static Stream<String> lines(String uri, Charset charset) throws IOException {
        FileObject fileObject = get(uri);
        return lines(fileObject, charset).onClose(new UncheckedRunnable(fileObject));
    }

    /**
     * 按行读取文件
     */
    public static Stream<String> lines(FileObject fileObject, Charset charset) throws IOException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(newInputStream(fileObject), charset));
            return reader.lines().onClose(new UncheckedRunnable(reader));
        } catch (RuntimeException e) {
            if (reader != null) {
                reader.close();
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
