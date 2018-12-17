package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.AliyunOSSProcessImage;
import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import cc.whohow.vfs.tree.FileTree;
import cc.whohow.vfs.tree.TreeBreadthFirstIterator;
import cc.whohow.vfs.tree.TreePostOrderIterator;
import cc.whohow.vfs.tree.TreePreOrderIterator;
import org.junit.Test;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestTree {
    @Test
    public void testPreOrder() {
        new FileTree(new File("."), TreePreOrderIterator::new)
                .forEach(System.out::println);
    }

    @Test
    public void testPostOrder() {
        new FileTree(new File("."), TreePostOrderIterator::new)
                .forEach(System.out::println);
    }

    @Test
    public void testBreadthFirst() {
        new FileTree(new File("."), TreeBreadthFirstIterator::new)
                .forEach(System.out::println);
    }

    @Test
    public void testAssign() {
        System.out.println(Object.class.isAssignableFrom(String.class));
        System.out.println(ProcessImage.class.isAssignableFrom(AliyunOSSProcessImage.class));
    }

    @Test
    public void testPattern() {
        Pattern URI_PATTERN = Pattern.compile("^((?<scheme>.+)://)?([^@#&*?]*)(?<reserved>[@#&*?])?");
        String text = "http://example.com/a.jpg";
        Matcher matcher = URI_PATTERN.matcher(text);
        if (matcher.find()) {
            System.out.println("find");
        }
        System.out.println(matcher.group("scheme"));
        System.out.println(matcher.group("reserved"));
    }
}
