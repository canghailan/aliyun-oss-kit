package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.vfs.operations.AliyunOSSProcessImage;
import cc.whohow.aliyun.oss.vfs.operations.ProcessImage;
import cc.whohow.vfs.tree.FileTree;
import cc.whohow.vfs.tree.TreeBreadthFirstIterator;
import cc.whohow.vfs.tree.TreePostOrderIterator;
import cc.whohow.vfs.tree.TreePreOrderIterator;
import org.junit.Test;

import java.io.File;

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
}
