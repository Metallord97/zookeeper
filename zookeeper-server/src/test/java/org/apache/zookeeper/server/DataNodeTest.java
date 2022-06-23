package org.apache.zookeeper.server;

import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

public class DataNodeTest {

    private static final char[] INVALID_CHARACTERS = {
            '\u0000',
            '\u0001',
            '\u001F',
            '\u007F',
            '\u009F',
            '\ud800',
            '\uF8FF',
            '\uFFF0',
            '\uFFFF',
    };

    private static Random random;
    private DataNode node;

    @BeforeAll
    public static void configureRandomGenerator() {
        DataNodeTest.random = new Random(System.currentTimeMillis());
    }

    @BeforeEach
    public void configureDataNode() {
        this.node = new DataNode();
    }

    @Disabled("Disabled because addChild method allow to insert invalid character")
    @Test
    public void shouldThrowExceptionAddChildInvalidCharacter() {
        for(char ch: INVALID_CHARACTERS) {
            Assertions.assertThrows(Exception.class, () -> this.node.addChild("/invalid" + ch));
        }
    }

    @Disabled("Disabled because addChild allow to insert empty string")
    @Test
    public void shouldThrowExceptionOnEmptyString() {
        Assertions.assertThrows(Exception.class, ()-> this.node.addChild(""));
    }

    @Disabled("Disabled because addChild allow to insert null value")
    @Test
    public void shouldThrowExceptionOnNull() {
        Assertions.assertThrows(Exception.class, ()->this.node.addChild(null));
    }

    @Test
    public void testAddChildValidString() {
        Assertions.assertTrue(this.node.addChild("/abc"));
        Assertions.assertFalse(this.node.addChild("/abc"));
        Assertions.assertTrue(this.node.removeChild("/abc"));
        Assertions.assertFalse(this.node.removeChild("/abc"));
    }

    @Test
    public void testRemoveChildren() {
        String path = "/validpath";
        Assertions.assertFalse(this.node.removeChild(null));
        Assertions.assertFalse(this.node.removeChild("/asd"));
        this.node.addChild(path);
        Assertions.assertTrue(this.node.removeChild(path));
        Assertions.assertFalse(this.node.removeChild(path));
    }

    @Disabled("Disabled because setChildren method doesn't check for invalid character")
    @Test
    public void testSetChildrenShouldThrowException() {
        HashSet<String> children = new HashSet<>();
        //Adding valid path
        children.add("/abc");
        children.add("/cde");
        //This is not valid
        children.add("/qwerty" + INVALID_CHARACTERS[random.nextInt(INVALID_CHARACTERS.length)] + "valid characters");

        Assertions.assertThrows(Exception.class, ()-> node.setChildren(children));
    }

    @Test
    public void testSetChildrenValidPath() {
        HashSet<String> children = new HashSet<>();
        children.add("/abc");
        children.add("/def");
        children.add("/ghil");
        this.node.setChildren(children);
        Assertions.assertEquals(children, this.node.getChildren());

    }

    @Test
    public void testGetChildrenEmptyNode() {
        Assertions.assertEquals(Collections.EMPTY_SET, this.node.getChildren());
        this.node.setChildren(null);
        Assertions.assertEquals(Collections.EMPTY_SET, this.node.getChildren());
    }

    @Disabled("zookeeper token is reserved, then addChild should not allow to insert it")
    @Test
    public void testZookeeperTokenAddChild() {
        String token = "/zookeeper";
        Assertions.assertThrows(Exception.class, () -> node.addChild(token));
    }

}
