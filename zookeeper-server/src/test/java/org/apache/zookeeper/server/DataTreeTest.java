package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class DataTreeTest {
    private static Random random;
    private DataTree tree;

    @BeforeAll
    public static void configureRandom() {
        DataTreeTest.random = new Random(System.currentTimeMillis());
    }

    @BeforeEach
    public void configureTree() {
        this.tree = new DataTree();
    }

    @Test
    public void testCreateNodeValidData() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/abc";
        byte[] validData = new byte[] {1,2,3};
        List<ACL> acl = AclParser.parse("world:anyone:r");
        Stat stat = new Stat();
        this.tree.createNode(validPath, validData, acl, -1, -1, 1, 1, stat);
        Assertions.assertArrayEquals(validData, this.tree.getData(validPath, stat, null));
        Assertions.assertEquals(acl, this.tree.getACL(validPath, stat));
    }

    @Test
    public void testCreateNodeEmptyACL() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/def";
        byte [] validData = new byte[random.nextInt(1000)];
        random.nextBytes(validData);
        List<ACL> emptyList = Collections.emptyList();
        Stat stat = new Stat();
        this.tree.createNode(validPath, validData, emptyList, Long.MIN_VALUE, 1, 1, 1, stat);
        Assertions.assertArrayEquals(validData, this.tree.getData(validPath, stat, null));
        Assertions.assertEquals(emptyList, this.tree.getACL(validPath, stat));
        Assertions.assertTrue(this.tree.getContainers().contains(validPath));
    }

    @Test
    public void testCreateNodeACLNull() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/def";
        byte [] validData = new byte[random.nextInt(1000)];
        random.nextBytes(validData);
        Stat stat = new Stat();
        this.tree.createNode(validPath, validData, null, 1, 0, 0, 0, stat);
        Assertions.assertEquals(validData, this.tree.getData(validPath, stat, null));
        List<ACL> expectedValue = new ReferenceCountedACLCache().convertLong(-1L);
        //Quando viene passato null come parametro per la lista acl in createNode viene impostata la acl con tutti i permessi
        Assertions.assertEquals(expectedValue, this.tree.getACL(validPath, stat));
    }

    @Test
    public void testCreateNodeInvalidPath() {
        String invalidPath = "/abc/def";
        byte[] data = getRandomData(random.nextInt(1000));
        Assertions.assertThrows(KeeperException.NoNodeException.class,
                ()->this.tree.createNode(invalidPath, data, null, 1, 1, 1, 1, null));
    }

    @Test
    public void testCreateNodeExistingPath() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/abc";
        byte[] data = getRandomData(random.nextInt(1000));
        this.tree.createNode(validPath, data, null, 1, 1, 1, 1, null);
        data = getRandomData(random.nextInt(1000));
        byte[] finalData = data;
        Assertions.assertThrows(KeeperException.NodeExistsException.class,
                ()->this.tree.createNode(validPath, finalData, null, 1, 1, 1, 1,null));
    }

    @Disabled("Disabled because Zookeeper should not allow to create node with data size greater than 1MB")
    @Test
    public void testCreateNodeDataMaxValue() {
        String validPath = "/abc";
        byte[] arrayWithMaxSize = getRandomData(1048577);
        Assertions.assertThrows(Exception.class,
                ()->this.tree.createNode(validPath, arrayWithMaxSize, null, 1, 1, 1, 1, null));
    }

    @Disabled("Disabled because empty string should be the root path")
    @Test
    public void testCreateNodeEmptyPath() {
        String empty = "";
        byte[] data = getRandomData(1000);
        Assertions.assertThrows(KeeperException.NodeExistsException.class,
                ()->this.tree.createNode(empty, data, null, -1, 1, 1, 1, null));
    }

    @Test
    public void testCreateNodePathWithoutSlash() {
        String invalidPath = "abc";
        byte[] data = getRandomData(2000);
        Assertions.assertThrows(Exception.class,
                ()->this.tree.createNode(invalidPath, data, null, 0, 0, 0,0,null));
    }

    @Test
    public void testCreateNodeNullData() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/abc";
        Stat stat = new Stat();
        this.tree.createNode(validPath, null, null, -1, 1, 1, 1, stat);
        Assertions.assertNull(this.tree.getData(validPath, stat, null));
    }

    @Test
    public void testCreateNodeEmptyDataArray() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/abc";
        Stat stat = new Stat();
        this.tree.createNode(validPath, new byte[]{}, null, -1, 0, 1, 0, stat);
        Assertions.assertArrayEquals(new byte[]{}, this.tree.getData(validPath, stat, null));
    }

    @Test
    public void testCreateNodeQuotaAndLimit() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parent = Quotas.quotaZookeeper + "/parent";
        String limitNode = parent + "/" + Quotas.limitNode;
        byte [] expectedData = getRandomData(100);
        Stat stat = new Stat();
        this.tree.createNode(parent, new byte[0], null, -1, 0, 1, 0);
        this.tree.createNode(limitNode, expectedData, null, -1, 0, 1, 0, stat);
        Assertions.assertArrayEquals(expectedData, this.tree.getData(limitNode, stat, null));
    }

    @Test
    public void testCreateNodeQuotaAndStat() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parent = Quotas.quotaZookeeper + "/parent";
        String statNode = parent + "/" + Quotas.statNode;
        Stat stat = new Stat();
        this.tree.createNode(parent, new byte[0], null, -1, 0, 1, 0);
        this.tree.createNode(statNode, null, null, -1, 0, 1, 0, stat);
        Assertions.assertNotNull( this.tree.getNode(statNode));
    }

    @Test
    public void testDeleteNodeValidPath() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String validPath = "/abc";
        this.tree.createNode(validPath, null, null, 1, 1, 1, 0);
        Assertions.assertNotNull(this.tree.getNode(validPath));
        this.tree.deleteNode(validPath, 1);
        Assertions.assertNull(this.tree.getNode(validPath));
    }

    @Test
    public void testDeleteNodeInvalidPath() {
        String invalidPath = "/def"; // this path doesn't math to any nodes
        Assertions.assertThrows(KeeperException.NoNodeException.class, ()->this.tree.deleteNode(invalidPath, 0));
    }

    @Test
    public void testDeleteNodeDeleteParentFirst() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parentPath = Quotas.quotaZookeeper + "/parent";
        String childPath = parentPath + "/child";
        this.tree.createNode(parentPath, null, null, Long.MIN_VALUE, 1, 1, 1);
        this.tree.createNode(childPath, null, null, 1, 1, 1, 1);
        Assertions.assertTrue(this.tree.getContainers().contains(parentPath));
        this.tree.deleteNode(parentPath, Integer.MAX_VALUE);
        Assertions.assertThrows(KeeperException.NoNodeException.class, ()->this.tree.deleteNode(childPath, 1));
        Assertions.assertFalse(this.tree.getContainers().contains(parentPath));
    }

    @Test
    public void testDeleteNodeTTLEphemeral() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        System.setProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY, "true");
        System.setProperty(EphemeralType.TTL_3_5_3_EMULATION_PROPERTY, "true");

        String node = "/node";
        Stat stat = new Stat();
        this.tree.createNode(node, null, null, EphemeralType.TTL.toEphemeralOwner(1), 1, 1, 1, stat);
        Assertions.assertTrue(this.tree.getTtls().contains(node));
        this.tree.deleteNode(node, 1);
        Assertions.assertFalse(this.tree.getTtls().contains(node));
        Assertions.assertNull(this.tree.getNode(node));
    }

    @Test
    public void testDeleteNodeNull() {
        Assertions.assertThrows(Exception.class, ()->this.tree.deleteNode(null, -1));
    }

    @Test
    public void testDeleteNodeEmptyString() {
        Assertions.assertThrows(Exception.class, ()->this.tree.deleteNode("", 1));
    }

    @Test
    public void testCreateNodeCheckParentCVersion() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parent = "/parent";
        String child = parent + "/child";

        Stat parentStat = new Stat();
        this.tree.createNode(parent, null, null, 1, 0, 1, 1, parentStat);
        this.tree.createNode(child, null, null, -1, -1, 2, 1);
        parentStat.setCversion(parentStat.getCversion() + 1);
        parentStat.setNumChildren(parentStat.getNumChildren() + 1);
        parentStat.setPzxid(2);
        Stat actualParentStat = new Stat();
        this.tree.getNode(parent).copyStat(actualParentStat);
        Assertions.assertEquals(parentStat, actualParentStat);
    }

    @Test
    public void testCreateNodeSameCVersion() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parent = "/parent";
        String child = parent +"/child";

        Stat parentStat = new Stat();
        this.tree.createNode(parent, null, null, 1, 1, 1, 1, parentStat);
        this.tree.createNode(child, null, null, 1, 0, 2, 2);
        parentStat.setNumChildren(parentStat.getNumChildren() + 1);
        parentStat.setCversion(parentStat.getCversion()*2 - 1);
        Stat actualParentStat = new Stat();
        this.tree.getNode(parent).copyStat(actualParentStat);
        Assertions.assertEquals(parentStat, actualParentStat);
    }

    @Test
    public void testCreateNodeEphemeralList() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String node = "/node";
        long ephemeralOwner = 1;
        this.tree.createNode(node, null, null, ephemeralOwner, 1, 1, 1);
        Assertions.assertTrue(this.tree.getEphemerals(ephemeralOwner).contains(node));
        this.tree.deleteNode(node, 2);
        Assertions.assertFalse(this.tree.getEphemerals(ephemeralOwner).contains(node));
    }

    @Test
    public void testDeleteNodeCheckParentPzxid() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String parent = "/parent";
        String child = parent + "/child";
        long zxidDelete = 3;
        this.tree.createNode(parent, null, null, 1, 1, 2, 1);
        this.tree.createNode(child, null, null, 1, 1, 1, 1);
        this.tree.deleteNode(child, zxidDelete);
        Stat stat = new Stat();
        this.tree.getNode(parent).copyStat(stat);
        Assertions.assertEquals(zxidDelete, stat.getPzxid());
    }

    @Test
    public void testDeleteCachedDataSize() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        String node = "/node";

        long nodeDataSize = this.tree.cachedApproximateDataSize();

        this.tree.createNode(node, null, null, 1, 1, 1, 1);
        Assertions.assertNotEquals(nodeDataSize, this.tree.cachedApproximateDataSize());

        this.tree.deleteNode(node, 1);
        Assertions.assertEquals(nodeDataSize, this.tree.cachedApproximateDataSize());
    }


    private static byte[] getRandomData(int length) {
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }
}
