package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.path.PathBuilder;
import cc.whohow.aliyun.oss.path.PathParser;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class UriFileName implements FileName {
    private final URI uri;

    public UriFileName(String uri) {
        this(URI.create(uri));
    }

    public UriFileName(URI uri) {
        this.uri = uri;
    }

    @Override
    public String getBaseName() {
        return new PathParser(uri.getPath()).getLastName();
    }

    @Override
    public String getPath() {
        return uri.getPath();
    }

    @Override
    public String getPathDecoded() {
        return uri.getPath();
    }

    @Override
    public String getExtension() {
        return new PathParser(uri.getPath()).getExtension();
    }

    @Override
    public int getDepth() {
        return new PathParser(uri.getPath()).getNameCount();
    }

    @Override
    public String getScheme() {
        return uri.getScheme();
    }

    @Override
    public String getURI() {
        return uri.toString();
    }

    @Override
    public String getRootURI() {
        return getRoot().toString();
    }

    @Override
    public FileName getRoot() {
        return new UriFileName(newUri("/"));
    }

    @Override
    public FileName getParent() {
        String parent = new PathParser(uri.getPath()).getParent();
        if (parent == null) {
            return null;
        }
        return new UriFileName(newUri(parent));
    }

    @Override
    public String getRelativeName(FileName name) throws FileSystemException {
        if (name instanceof UriFileName) {
            UriFileName that = (UriFileName) name;
            if (Objects.equals(that.uri.getScheme(), this.uri.getScheme()) &&
                    Objects.equals(that.uri.getHost(), this.uri.getHost())&&
                    Objects.equals(that.uri.getPort(), this.uri.getPort())) {
                return new PathBuilder(this.uri.getPath()).relativize(that.uri.getPath()).toString();
            }
        }
        throw new FileSystemException("");
    }

    @Override
    public boolean isAncestor(FileName ancestor) {
        if (ancestor instanceof UriFileName) {
            UriFileName that = (UriFileName) ancestor;
            if (Objects.equals(that.uri.getScheme(), this.uri.getScheme()) &&
                    Objects.equals(that.uri.getHost(), this.uri.getHost())&&
                    Objects.equals(that.uri.getPort(), this.uri.getPort())) {
                return uri.getPath().startsWith(that.uri.getPath());
            }
        }
        return false;
    }

    @Override
    public boolean isDescendent(FileName descendant) {
        if (descendant instanceof UriFileName) {
            UriFileName that = (UriFileName) descendant;
            if (Objects.equals(that.uri.getScheme(), this.uri.getScheme()) &&
                    Objects.equals(that.uri.getHost(), this.uri.getHost())&&
                    Objects.equals(that.uri.getPort(), this.uri.getPort())) {
                return descendant.getPath().startsWith(that.uri.getPath());
            }
        }
        return false;
    }

    @Override
    public boolean isDescendent(FileName descendant, NameScope nameScope) {
        return isDescendent(descendant);
    }

    @Override
    public boolean isFile() {
        return getType() == FileType.FILE;
    }

    @Override
    public FileType getType() {
        if (uri.getPath() == null ||
                uri.getPath().isEmpty() ||
                uri.getPath().equals("/")) {
            return FileType.FOLDER;
        } else{
            return FileType.FILE;
        }
    }

    @Override
    public String getFriendlyURI() {
        return uri.toString();
    }

    @Override
    public int compareTo(FileName o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof UriFileName) {
            UriFileName that = (UriFileName) o;
            return that.uri.equals(this.uri);
        }
        return false;
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    private URI newUri(String path) {
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, null, null);
        } catch (URISyntaxException e) {
            throw new UndeclaredThrowableException(e);
        }
    }
}
