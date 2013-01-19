package org.phicluster.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.*;
import org.phicluster.core.task.PhiTask;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;


public class DistTaskPoolTest {
    protected static final Logger logger = LoggerFactory.getLogger(DistTaskPoolTest.class);
    
    private static ZKInit zkInit;
    private static MockKernel kernel1;
    private static MockKernel kernel2;
    private static MockKernel kernel3;
    
    private static MockAccrualFailureDetector mockAccrualFailureDetector;

    private DistTaskPool distTaskPool;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(true);

        // creating kernels for three nodes
        logger.info("creating kernels of three nodes...");
        kernel1 = CoreSetup.createMockKernel(1);// new MockKernel(1);
        kernel2 = CoreSetup.createMockKernel(2);// new MockKernel(2);
        kernel3 = CoreSetup.createMockKernel(3);// new MockKernel(3);
        
        mockAccrualFailureDetector = new MockAccrualFailureDetector(kernel1.nodeId(), kernel1.zk, 0);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("tearing down unit test....");
        
        // clean kernel data in zk
        kernel1.tearDown();
        kernel2.tearDown();
        kernel3.tearDown();
        
        // clean cluster data in zk
        zkInit.tearDown();
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
        this.distTaskPool = new DistTaskPool(kernel1.nodeId(), kernel1.zk, mockAccrualFailureDetector);
        DistTaskPool.defaultInstance = this.distTaskPool;
    }

    @After
    public void tearDown() throws Exception {
        // make sure it is set back to default value after each test
        mockAccrualFailureDetector.numberOfNodes = 3; 
    }
    
    @Test
    public void testGetReplicationNodes() {
        try {
            // expect all three nodes 
            Set<String> nodesSelected = distTaskPool.getReplicationNodes();
            Set<String> expected = new HashSet<String>(mockAccrualFailureDetector.availableNodes().values());
            assertEquals(expected.size(), nodesSelected.size());
            assertTrue(nodesSelected.containsAll(expected));
            
            // expect two of five other nodes selected randomly
            mockAccrualFailureDetector.numberOfNodes = 6; // this field is only in mock class
            Set<String> allNodes = new HashSet<String>(mockAccrualFailureDetector.availableNodes().values());
            Map<String, Integer> counts = new HashMap<String, Integer>();
            for (int i = 0; i < 100; i++) {
                nodesSelected = distTaskPool.getReplicationNodes();
                assertEquals(2, nodesSelected.size());
                assertTrue(allNodes.containsAll(nodesSelected));
                for (String node : nodesSelected) {
                    Integer count = counts.get(node);
                    if (count == null) {
                        count = 0;
                    }
                    count += 1;
                    counts.put(node, count);
                }
            }
            // we should expect each node is selected more than once
            // after 100 itereations
            for (String node : allNodes) {
                logger.info("count[{}]: {}", node, counts.get(node));
                assertNotNull("node: " + node + " has never been selected", counts.get(node));
                assertTrue("node: " + node + " has always been selected", counts.get(node) < 100);
            }
            
            
            // expect only one other node to replicate to 
            mockAccrualFailureDetector.numberOfNodes = 2; // this field is only in mock class
            expected = new HashSet<String>(mockAccrualFailureDetector.availableNodes().values());
            nodesSelected = distTaskPool.getReplicationNodes();
            assertEquals(expected.size(), nodesSelected.size());
            assertEquals(1, nodesSelected.size());
            assertTrue(nodesSelected.containsAll(expected));
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception in getting nodes: " + e.getMessage());
        }
        
    }
    
    @Test
    public void testDeleteZNodes() {
        // create list of nodes to be deleted
        List<String> paths = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            String p = "/unittest-distpooltask-deleteznodes-p" + i;
            try {
                distTaskPool.zk.create(p, new byte[0], distTaskPool.acl, CreateMode.EPHEMERAL);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                fail("failed to create znode: " + p + ", exception: " + e.getMessage());
            }
            paths.add(p);
        }
        // now delete these znodes
        try {
            distTaskPool.deleteZNodes(paths);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            fail("failed deleting znodes: " + e.getMessage());
        }
        // make sure all nodes are deleted
        for (String p : paths) {
            try {
                Stat stat = distTaskPool.zk.exists(p, false);
                assertNull("path still exists: " + p, stat);
            } catch (KeeperException | InterruptedException e) {
                fail("exception in validating the path " + p + " has been deleted: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void testReplicate() {
        List<String> pathsCreated = null;
        try {
            pathsCreated = distTaskPool.replicate(198L);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }

        assertNotNull("no paths created", pathsCreated);
        for (String p : pathsCreated) {
            logger.info("path created: {}", p);
        }
        
        assertEquals(2, pathsCreated.size());
        
        String path1 = "/tasks/secondary/node-2/task-198";
        assertTrue("path not found but expected in created paths: " + path1, pathsCreated.contains(path1));
        String path2 = "/tasks/secondary/node-3/task-198";
        assertTrue("path not found but expected in created paths: " + path2, pathsCreated.contains(path2));
        
        // check the existence of the paths in zk
        try {
            Stat stat = distTaskPool.zk.exists(path1, false);
            assertNotNull("path does not exist: " + path1, stat);
            stat = distTaskPool.zk.exists(path2, false);
            assertNotNull("path does not exist: " + path2, stat);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        // it has succeeded! now clean up
        logger.info("replication succeeded; cleaning up...");
        try {
            distTaskPool.zk.delete(path1, -1);
            logger.info("path deleted: {}", path1);
            distTaskPool.zk.delete(path2, -1);
            logger.info("path deleted: {}", path1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            fail("exception during cleanup");
        }
        logger.info("done");
    }
    
    @Test
    public void testCreateTaskInQueue() {
        long taskId = 19819823;
        try {
            distTaskPool.createTaskInQueue(taskId);
            logger.info("task creaded in queue: {}", taskId);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        String expectedPath = "/tasks/queue/task-" + taskId;
        try {
            Stat stat = kernel1.zk.exists(expectedPath, false);
            assertNotNull("path doesn not exist: " + expectedPath, stat);
            logger.info("path for task in queue: {}", expectedPath);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
        // succeeded, now clean up
        try {
            kernel1.zk.delete(expectedPath, -1);
            logger.info("deleted: {}", expectedPath);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            fail("exception while deleting: " + e.getMessage());
        }
        logger.info("done");
    }
    
    @Test
    public void testOffer() {
        String jsonPhiTask = "unittest-task";
        try {
            PhiTask td = distTaskPool.offer(jsonPhiTask);
            logger.info("task created, id: {}", td.taskId);

            String expectedPath = Constants.ZN_TASK_DATA_PATH_WITH_PREFIX + String.format("%010d", td.taskId);
            Stat stat = kernel1.zk.exists(expectedPath, false);
            assertNotNull("expected path for task doesn't exist" + expectedPath, stat);
            logger.info("task path: {}", expectedPath);
            
            String expectedQueuePath = Constants.ZN_TASK_QUEUE + "/" + Constants.PREFIX_TASK + td.taskId;
            stat = kernel1.zk.exists(expectedQueuePath, false);
            assertNotNull("expected queue path for task doesn't exist: " + expectedQueuePath, stat);
            logger.info("task queue path: {}", expectedQueuePath);
            
            // clean up...
            kernel1.zk.delete(expectedQueuePath, -1);
            logger.info("cleaning up, delete: {}", expectedQueuePath);
            kernel1.zk.delete(expectedPath, -1);
            logger.info("cleaning up, delete: {}", expectedPath);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        logger.info("done");
    }

    @Test
    public void testTake() {
        // this test requires an empty task queue to start with
        ensureEmptyQueue();
        
        // test plain-vanilla happy path
        logger.info("test plain-vanilla happy path...");
        List<PhiTask> tasks = createTasks(5);
        
        PhiTask taskTaken = null;
        try {
            taskTaken = distTaskPool.take(null);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception while taking a task from dist pool: " + e.getMessage());
        }
        assertNotNull("no task taken while expecting task-" + tasks.get(0).taskId, taskTaken);
        logger.info("task taken from dist task pool: task-{}", taskTaken.taskId);
        long expectedTaskId = tasks.get(0).taskId;
        assertEquals(expectedTaskId, taskTaken.taskId);
        
        try {
            // check replication
            String pathRep1 = "/tasks/secondary/node-2/task-" + taskTaken.taskId;
            String pathRep2 = "/tasks/secondary/node-3/task-" + taskTaken.taskId;
            Stat stat = distTaskPool.zk.exists(pathRep1, false);
            assertNotNull("path does not exist: " + pathRep1, stat);
            logger.info("task replicated to: {}", pathRep1);
            stat = distTaskPool.zk.exists(pathRep2, false);
            assertNotNull("path does not exist: " + pathRep2, stat);
            logger.info("task replicated to: {}", pathRep2);

            // check taken list entry
            String pathTaken = "/tasks/taken/task-" + taskTaken.taskId;
            stat = distTaskPool.zk.exists(pathTaken, false);
            assertNotNull("path does not exist: " + pathTaken, stat);
            logger.info("task is put in taken tasks list: {}", pathTaken);
            byte[] data = distTaskPool.zk.getData(pathTaken, false, stat);
            int ownerId = ByteUtil.readInt(data, 0);
            assertEquals("task taken owner id does not match", distTaskPool.nodeId, ownerId);
            
            // check task is removed from queue
            String queuePath = "/tasks/queue/task-" + taskTaken.taskId;
            stat = distTaskPool.zk.exists(queuePath, false);
            assertNull("task is still in queue: " + queuePath, stat);
            logger.info("task is removed from queue, path no longer exists: {}", queuePath);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        logger.info("happy path test done.");
        
        logger.info("test when first taken task is already replicated to one of the nodes");
        PhiTask nextTask = tasks.get(1); // next task in the sorted list
        
        // simulate replication by another node
        final String repPath = "/tasks/secondary/node-2/task-" + nextTask.taskId;
        try {
            distTaskPool.zk.create(repPath, ByteUtil.toBytes(3), distTaskPool.acl, CreateMode.PERSISTENT);
            logger.info("task is replicated into node-2 secondary task list: {}", repPath);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception while creating secondary task in node-2: " + repPath);
        }
        
        // attempt to take a task
        try {
            PhiTask td = distTaskPool.take(null);
            expectedTaskId = tasks.get(2).taskId;
            assertNotNull("no task taken while expecting task: " + expectedTaskId, td);
            assertEquals(expectedTaskId, td.taskId);
            logger.info("task taken: {}", td.taskId);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception while taking a task from dist task pool: " + e.getMessage());
        }
        
        // make sure no replication remains for the other task
        try {
            String repPath3 = "/tasks/secondary/node-3/task-" + nextTask.taskId;
            Stat stat = distTaskPool.zk.exists(repPath3, false);
            assertNull("not expected to exists: " + repPath3, stat);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        try {
            distTaskPool.zk.delete(repPath, -1);
            logger.info("task replica in node-2 secondary task list deleted: {}", repPath);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            fail("exception while deleting " + repPath + ": " + e.getMessage());
        }
        logger.info("test to skip due to replicated to another node is done.");
        
        // test tasks taken skipped
        logger.info("test task is in already taken list");
        final String takenPath1 = "/tasks/taken/task-" + tasks.get(1).taskId;
        final String takenPath2 = "/tasks/taken/task-" + tasks.get(3).taskId;
        try {
            distTaskPool.zk.create(takenPath1, ByteUtil.toBytes(2), distTaskPool.acl, CreateMode.PERSISTENT);
            distTaskPool.zk.create(takenPath2, ByteUtil.toBytes(3), distTaskPool.acl, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        try {
            PhiTask td = distTaskPool.take(null);
            long expected = tasks.get(4).taskId;
            assertNotNull("no task returned from dist task pool while expected: " + expected, td);
            assertEquals(expected, td.taskId);
            logger.info("task taken: {}", td.taskId);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception while taking a task: " + e.getMessage());
        }
        
        try {
            // check replicas do not exists for the skipped tasks
            String pathRep1_1 = "/tasks/secondary/node-2/task-" + tasks.get(1).taskId;
            String pathRep1_2 = "/tasks/secondary/node-3/task-" + tasks.get(1).taskId;
            String pathRep2_1 = "/tasks/secondary/node-2/task-" + tasks.get(3).taskId;
            String pathRep2_2 = "/tasks/secondary/node-3/task-" + tasks.get(3).taskId;
            String[] paths = {pathRep1_1, pathRep1_2, pathRep2_1, pathRep2_2};
            for (String p : paths) {
                Stat stat = distTaskPool.zk.exists(p, false);
                assertNull("path exists but not expecte: " + p, stat);
            }
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        logger.info("test tasks taken skipped done.");

        // clean up
        logger.info("tests completed, cleaning up...");
        cleanUpTasks(tasks);
        logger.info("clean up completed.");

        logger.info("done");
    }
    
    @Test
    public void testMarkTaskDone() {
        // this test requires empty task queue to start with
        ensureEmptyQueue();
        
        logger.info("creating tasks for test");
        List<PhiTask> tasks = createTasks(2);
        
        // happy path test
        logger.info("test-1 - happy path");
        PhiTask taskTaken = null;
        try {
            taskTaken = distTaskPool.take(null);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception while taking a task from dist pool: " + e.getMessage());
        }
        assertNotNull("no task taken while expecting task-" + tasks.get(0).taskId, taskTaken);
        logger.info("task taken from dist task pool: task-{}", taskTaken.taskId);
        long expectedTaskId = tasks.get(0).taskId;
        assertEquals(expectedTaskId, taskTaken.taskId);
        
        try {
            distTaskPool.markTaskDone(taskTaken.taskId);
            
            String p = "/tasks/data/task-"+String.format("%010d", taskTaken.taskId) + "/done";
            Stat stat = distTaskPool.zk.exists(p, false);
            assertNotNull("path to mark task done does not exist: " + p, stat);
            logger.info("path exists: {}", p);

            byte[] data = distTaskPool.zk.getData(p, false, stat);
            int ownerId = ByteUtil.readInt(data, 0);
            assertEquals("unexpected node id for completed task", distTaskPool.nodeId, ownerId);
            logger.info("task marked done by node: {}", ownerId);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        logger.info("done - test-1");
        
        // test marking a task owned by another node
        logger.info("test-2 - attempt to mark a task owned by another task");
        long takenTaskId = tasks.get(1).taskId;
        final String takenPath = "/tasks/taken/task-" + takenTaskId;
        try {
            distTaskPool.zk.create(takenPath, ByteUtil.toBytes(2), distTaskPool.acl, CreateMode.PERSISTENT);
            logger.info("task is taken by node-{}: {}", 2, takenPath);            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        try {
            distTaskPool.markTaskDone(takenTaskId);
            fail("exception expected while trying to mark task-" + takenTaskId + " done.");
        } catch (Exception e) {
            // exception expected!
            logger.info("expected exception received while marking task-{} done: {}", 
                        takenTaskId, e.getMessage());
        }
        logger.info("done - test-2");

        // clean up
        logger.info("tests completed, cleaning up...");
        cleanUpTasks(tasks);
        logger.info("clean up completed.");

        logger.info("done");        
    }
    
    private void ensureEmptyQueue() {
        try {
            List<String> queue = distTaskPool.zk.getChildren("/tasks/queue", false);
            assertTrue("task queue is not empty", queue.isEmpty());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception in reading task queue: " + e.getMessage());
        }
    }
    
    private void deleteIfExists(String path) throws Exception {
        Stat stat = distTaskPool.zk.exists(path, false);
        if (stat != null) {
            distTaskPool.zk.delete(path, -1);
            logger.info("deleted: {}", path);
        } else {
            logger.info("does not exist: {}", path);
        }
    }
    
    private List<PhiTask> createTasks(int numberOfTasks) {
        List<PhiTask> tasks = new ArrayList<PhiTask>();
        for (int i = 0; i < numberOfTasks; i++) {
            String jsonPhiTask = "unittest-disttaskpool-task-" + i;
            PhiTask td = null;
            try {
                td = distTaskPool.offer(jsonPhiTask);
                tasks.add(td);
            } catch (Exception e) {
                e.printStackTrace();
                fail("failed creating task: " + jsonPhiTask);
            }
            logger.info("task created, id: {}", td.taskId);
        }

        return tasks;
    }

    private void cleanUpTasks(List<PhiTask> tasks) {
        try {
            for (PhiTask td : tasks) {
                for (int i = 1; i <= 3; i++) {
                    String p = "/tasks/secondary/node-"+i+"/task-" + td.taskId;
                    deleteIfExists(p);
                }
                String p = "/tasks/taken/task-" + td.taskId;
                deleteIfExists(p);
                p = "/tasks/queue/task-" + td.taskId;
                deleteIfExists(p);
                p = "/tasks/data/task-" + String.format("%010d", td.taskId);
                deleteIfExists(p+"/done");
                deleteIfExists(p);
            }            
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception during clean up: " + e.getMessage());
        }
    }
}
