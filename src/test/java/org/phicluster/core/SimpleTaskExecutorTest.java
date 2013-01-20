package org.phicluster.core;

import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.junit.*;
import org.phicluster.core.task.PhiTask;
import org.phicluster.core.task.simple.SimpleTask;
import org.phicluster.core.task.simple.TaskCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SimpleTaskExecutorTest {
    protected static final Logger logger = LoggerFactory.getLogger(SimpleTaskExecutorTest.class);

    private static ZKInit zkInit;
    private static MockKernel kernel;
    private static MockKernel kernel_backup;

    private DistTaskPool distTaskPool;
    private int taskCodeId;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(true);

        // creating kernels for two nodes
        logger.info("creating kernels of two nodes...");
        kernel = CoreSetup.createMockKernel(1);
        kernel_backup = CoreSetup.createMockKernel(2); // for task replication

        kernel.initDistTaskSystem();
        kernel.mockAccrualFailureDetector.numberOfNodes = 2;
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("tearing down unit test....");

        // clean kernel data in zk
        kernel.tearDown();
        kernel_backup.tearDown();

        // clean cluster data in zk
        zkInit.tearDown();
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
        distTaskPool = DistTaskPool.defaultInstance();
        // create a new task worker
        SimpleTask simpleTask = new SimpleTask();
        // add worker into task executor
        TaskCode code = TaskCode.getInstance();
        taskCodeId = code.addTaskCode(simpleTask);
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testExecutorViaPoolOffer() {
        // create a json request to submit
        String jsonRequest = getJsonRequestString(taskCodeId);

        logger.info("json task request: {}", jsonRequest);

        try {
            PhiTask td = distTaskPool.offer(jsonRequest);
            logger.info("task created, id: {}", td.taskId);

            String expectedPath = Constants.ZN_TASK_DATA_PATH_WITH_PREFIX + String.format("%010d", td.taskId);
            Stat stat = kernel.zk.exists(expectedPath, false);
            assertNotNull("expected path for task doesn't exist" + expectedPath, stat);
            logger.info("task path: {}", expectedPath);

            String expectedQueuePath = Constants.ZN_TASK_QUEUE + "/" + Constants.PREFIX_TASK + td.taskId;
            stat = kernel.zk.exists(expectedQueuePath, false);
            assertNotNull("expected queue path for task doesn't exist: " + expectedQueuePath, stat);
            logger.info("task queue path: {}", expectedQueuePath);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        logger.info("done");
    }

    // TODO: more test cases are needed

    private String getJsonRequestString(int tc) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("task-code", tc);
        jsonObject.put("job-id", 1);
        return jsonObject.toJSONString();
    }

}
