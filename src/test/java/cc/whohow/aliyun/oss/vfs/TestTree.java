package cc.whohow.aliyun.oss.vfs;

import cc.whohow.aliyun.oss.file.FileTree;
import cc.whohow.aliyun.oss.tree.TreeBreadthFirstIterator;
import cc.whohow.aliyun.oss.tree.TreePostOrderIterator;
import cc.whohow.aliyun.oss.tree.TreePreOrderIterator;
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
}
