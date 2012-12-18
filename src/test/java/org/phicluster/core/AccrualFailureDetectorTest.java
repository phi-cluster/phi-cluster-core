package org.phicluster.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.phicluster.core.AccrualFailureDetector;
import org.phicluster.core.Constants;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.ZKInit;
import org.phicluster.core.AccrualFailureDetector.HeartbeatMiss;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccrualFailureDetectorTest {
    protected static final Logger logger = LoggerFactory.getLogger(AccrualFailureDetectorTest.class);
    
    private static ZKInit zkInit;
    
    private static final int NUMBER_OF_KERNELS = 5;
    private static Map<Integer, MockKernel> kernels = new HashMap<>();
        
    private AccrualFailureDetector afd;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(MockKernel.ZOOKEEPER_FOR_UNIT_TEST, true);
        
        logger.info("creating kernels...");
        for (int i = 1; i <= NUMBER_OF_KERNELS; i++) {
            MockKernel kernel = CoreSetup.createMockKernel(i);
            kernels.put(i, kernel);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("tearing down unit test....");
        
        // clean kernel data in zk
        for (int i = 1; i <= NUMBER_OF_KERNELS; i++) {
            MockKernel kernel = kernels.get(i);
            kernel.tearDown();
        }
        
        // clean cluster data in zk
        zkInit.tearDown();
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
        MockKernel kernel = kernels.get(1); // this node
        afd = new AccrualFailureDetector(kernel.nodeId, kernel.zk, Constants.HEARTBEAT_INTERVAL);
        DistTaskPool distTaskPool = new DistTaskPool(1, kernel.zk, afd);
        DistTaskPool.defaultInstance = distTaskPool;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNodes() {
        logger.info("testNodes");
        
        try {
            Map<Integer, String> nodes = afd.nodes();
            logger.info("number of nodes: {}", nodes.size());
            assertEquals("unexpected number of nodes:", NUMBER_OF_KERNELS, nodes.size());
            for (int i = 1; i <= NUMBER_OF_KERNELS; i++) {
                String nodeName = nodes.get(i);
                assertNotNull("no node returned for node id: "+ i, nodeName);
                assertEquals("node name does not match", "node-" + i, nodeName);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }         
        logger.info("done.");
    }

    @Test
    public void testScanNodes() {
        MockKernel kernel = kernels.get(1); // this node

        try {
            afd.scanNodes(); // will init all counts to zero as it will read the initial heartbeat
            
            // scan nodes again, 
            // no heartbeat registered after initial, resulting in incrementing all nodes
            afd.scanNodes();
            for (int i = 2; i < NUMBER_OF_KERNELS; i++) {
                assertEquals("expected miss count=1 for node: " + i, new Integer(1), afd.heartbeatMisses(i));
            }

            // register a heartbeat for node 2:
            String p = "/heartbeat/node-2";
            kernel.zk.setData(p, ByteUtil.toBytes(System.currentTimeMillis()), -1);
            
            // scan again, which should reset miss counter for node-2 while incrementing for others
            afd.scanNodes();
            assertEquals("heartbeat miss counter was expected to be reset to zero.", new Integer(0), afd.heartbeatMisses(2));
            for (int i = 3; i < NUMBER_OF_KERNELS; i++) {
                assertEquals("expected miss count=1 for node: " + i, new Integer(2), afd.heartbeatMisses(i));
            }

            // register a heartbeat for node 3:
            p = "/heartbeat/node-3";
            kernel.zk.setData(p, ByteUtil.toBytes(System.currentTimeMillis()), -1);

            // scan again, which should reset miss counter for node-3 while incrementing for others
            afd.scanNodes();
            assertEquals("heartbeat miss counter was expected to be reset to zero for node-3.", new Integer(0), afd.heartbeatMisses(3));
            for (int i = 4; i < NUMBER_OF_KERNELS; i++) {
                assertEquals("expected miss count=1 for node: " + i, new Integer(3), afd.heartbeatMisses(i));
            }
            assertEquals("expected miss count=1 for node: 2", new Integer(1), afd.heartbeatMisses(2));

            // register another heartbeat for node 3:
            p = "/heartbeat/node-3";
            kernel.zk.setData(p, ByteUtil.toBytes(System.currentTimeMillis()), -1);
            // and, register a new heartbeat for node 2:
            p = "/heartbeat/node-2";
            kernel.zk.setData(p, ByteUtil.toBytes(System.currentTimeMillis()), -1);

            // now scanNodes should keep zero for node-3 miss count, 
            // and reset the counter for node-2 while incrementing for others
            afd.scanNodes();
            assertEquals("heartbeat miss counter was expected to stay zero for node-3.", new Integer(0), afd.heartbeatMisses(3));
            assertEquals("heartbeat miss counter was expected to be reset to zero for node-2.", new Integer(0), afd.heartbeatMisses(2));
            for (int i = 4; i < NUMBER_OF_KERNELS; i++) {
                assertEquals("expected miss count=1 for node: " + i, new Integer(4), afd.heartbeatMisses(i));
            }
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
    }

    @Test
    public void testAvailableNodes() {
        logger.info("testAvailableNodes");

        for (int i = 0; i < Constants.ACCRUAL_FAILURE_THRESHOLD-1; i++) {
            afd.incrementMissCount(2);
            afd.incrementMissCount(3);
            afd.incrementMissCount(4);
        }
        // increment node-2 misses to the threshold
        afd.incrementMissCount(2);
        // increment node-4 above the threshold
        afd.incrementMissCount(4);
        afd.incrementMissCount(4);
        
        try {
            Map<Integer, String> availableNodes = afd.availableNodes();
            assertEquals(NUMBER_OF_KERNELS-3, availableNodes.size());
            for (int i = 1; i <= NUMBER_OF_KERNELS; i++) {
                if (i == 1 || i == 2 || i == 4) {
                    String nodeName = availableNodes.get(i);
                    assertNull("unexpected node in available nodes: " + nodeName, nodeName);
                    continue;
                }
                String nodeName = availableNodes.get(i);
                assertNotNull("node with id: " + i + " was expected in available nodes", nodeName);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }
    
    @Test
    public void testReclaimTask() {
        logger.info("testReclaimTask");

        MockKernel kernel = kernels.get(1); // this node
        String secondaryTasksPath = "/tasks/secondary/node-1";
        Set<String> cleanupSet = createTasksInSecondaryList();
        
        // 1 - task not in taken list
        logger.info("case-1: task not in taken list");
        try {
            logger.info("reclaiming task-1...");
            boolean succeeded = afd.reclaimTask("task-1");
            assertFalse("expected to fail since task is not in taken list", succeeded);
            logger.info("reclaiming task-1 failed");
            
            // task must be removed from secondary task list
            String p = secondaryTasksPath+"/task-1";
            Stat stat = kernel.zk.exists(p, false);
            assertNull("task is still in secondary list: " + p, stat);
            logger.info("task-1 removed from secondary task list: {}", p);
            
            // make sure it is not created in queue
            p = "/tasks/queue/task-1";
            stat = kernel.zk.exists(p, false);
            assertNull("task is in queue: " + p, stat);
            logger.info("task-1 is not in task queue");
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        // 2 - task is in taken list but also exists in queue
        logger.info("case-2: task is in taken list but also exists in queue");
        try {
            String p = "/tasks/taken/task-3";
            kernel.zk.create(p, ByteUtil.toBytes(2), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created as part of test preparation: {}", p);
            p = "/tasks/queue/task-3";
            kernel.zk.create(p, new byte[0], kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created as part of test preparation: {}", p);
            
            logger.info("reclaiming task-3...");
            boolean succeeded = afd.reclaimTask("task-3");
            assertFalse("expected to fail since task is not in taken list", succeeded);
            logger.info("reclaiming task-3 failed");
            
            // task must be removed from secondary task list
            p = secondaryTasksPath+"/task-3";
            Stat stat = kernel.zk.exists(p, false);
            assertNull("task is still in secondary list: " + p, stat);
            logger.info("task-3 removed from secondary task list: {}", p);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        // 3 - task reclaimed successfully
        logger.info("case-3: task must be reclaimed successfully!");
        try {
            String p = "/tasks/taken/task-7";
            kernel.zk.create(p, ByteUtil.toBytes(3), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created as part of test preparation: {}", p);
            
            logger.info("reclaiming task-7...");
            boolean succeeded = afd.reclaimTask("task-7");
            assertTrue("reclaiming task-7 failed but expected to succeed", succeeded);
            logger.info("reclaiming task-7 succeeded!");
            
            // task must be in the queue
            p = "/tasks/queue/task-7";
            Stat stat = kernel.zk.exists(p, false);
            assertNotNull("task-7 is expected to be in the task queue but path " +
            		"does not exists" + p, stat);
            logger.info("task-7 is back in the task queue: {}", p);

            // task must be removed from taken tasks list
            p = "/tasks/taken/task-7";
            stat = kernel.zk.exists(p, false);
            assertNull("task-7 was expected to be removed from taken list but it " +
            		"still exists: " + p, stat);
            logger.info("task-7 removed from taken tasks list: path does not exist: {}", p);
            
            // task must be removed from secondary task list
            p = secondaryTasksPath+"/task-7";
            stat = kernel.zk.exists(p, false);
            assertNull("task is still in secondary list: " + p, stat);
            logger.info("task-3 removed from secondary task list: {}", p);
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }

        try {
            deleteIfExists(cleanupSet);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }
    
    private void deleteIfExists(Set<String> paths) throws Exception {
        MockKernel kernel = kernels.get(1); // this node
        for (String path : paths) {
            Stat stat = kernel.zk.exists(path, false);
            if (stat != null) {
                kernel.zk.delete(path, -1);
                logger.info("deleted: {}", path);
            } else {
                logger.info("does not exist: {}", path);
            }
        }
    }

    private Set<String> createTasksInSecondaryList() {
        MockKernel kernel = kernels.get(1); // this node
        String secondaryTasksPath = "/tasks/secondary/node-1";
        Set<String> cleanupSet = new HashSet<>();
        // add some arbitrary tasks
        try {
            logger.info("creating tasks in secondary tasks list of node-1");
            String p = kernel.zk.create(secondaryTasksPath+"/task-1", ByteUtil.toBytes(2), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created: {}", p);
            cleanupSet.add(p);
            p = kernel.zk.create(secondaryTasksPath+"/task-3", ByteUtil.toBytes(2), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created: {}", p);
            cleanupSet.add(p);
            p = kernel.zk.create(secondaryTasksPath+"/task-7", ByteUtil.toBytes(3), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created: {}", p);
            cleanupSet.add(p);
            p = kernel.zk.create(secondaryTasksPath+"/task-9", ByteUtil.toBytes(4), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created: {}", p);
            cleanupSet.add(p);
            p = kernel.zk.create(secondaryTasksPath+"/task-12", ByteUtil.toBytes(6), kernel.acl, CreateMode.PERSISTENT);
            logger.info("path created: {}", p);
            cleanupSet.add(p);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }

        return cleanupSet;
    }
    
    
    @Test
    public void testScanSecondaryTaskList() {
        logger.info("testScanSecondaryTaskList");

        MockKernel kernel = kernels.get(1); // this node
        Set<String> cleanupSet = createTasksInSecondaryList();

        try {
            // scan should eliminate task-12 since its owner node does not exist. 
            // all other tasks should be skipped as there are no heartbeat miss registered yet
            afd.scanSecondaryTaskList();
            String p = "/tasks/secondary/node-1/task-12";
            Stat stat = kernel.zk.exists(p, false);
            assertNull("task-12 was expected to be removed but still exists: " + p, stat);
            
            // other tasks must be still in the secondary list:
            p = "/tasks/secondary/node-1/task-1";
            stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);

            p = "/tasks/secondary/node-1/task-3";
            stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);

            p = "/tasks/secondary/node-1/task-7";
            stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);

            p = "/tasks/secondary/node-1/task-9";
            stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }

        try {
            afd.scanNodes(); // will result in reseting miss counters
            afd.scanNodes(); // will result in incrementing miss counters
            
            // register a heartbeat for node 2:
            String p = "/heartbeat/node-2";
            kernel.zk.setData(p, ByteUtil.toBytes(System.currentTimeMillis()), -1);

            for (int i = 1; i < Constants.ACCRUAL_FAILURE_THRESHOLD; i++) {
                afd.scanNodes();
            }
            
            // scan the list
            afd.scanSecondaryTaskList();
            
            // task-1 and task-3 must stay as node-2 should be considered up
            p = "/tasks/secondary/node-1/task-1";
            Stat stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);

            p = "/tasks/secondary/node-1/task-3";
            stat = kernel.zk.exists(p, false);
            assertNotNull("task was expected to be in the secondary tasks list but not: " + p, stat);

            // task-7 and task-9 must be tried to be reclaimed and removed from the list
            p = "/tasks/secondary/node-1/task-7";
            stat = kernel.zk.exists(p, false);
            assertNull("task was not expected to be in the secondary tasks: " + p, stat);

            p = "/tasks/secondary/node-1/task-9";
            stat = kernel.zk.exists(p, false);
            assertNull("task was not expected to be in the secondary tasks: " + p, stat);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        try {
            deleteIfExists(cleanupSet);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
    }
    
}
