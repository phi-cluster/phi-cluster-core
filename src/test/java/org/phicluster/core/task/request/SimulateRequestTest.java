package org.phicluster.core.task.request;

import org.gearman.*;
import org.junit.*;
import org.phicluster.core.CoreSetup;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.MockKernel;
import org.phicluster.core.ZKInit;
import org.phicluster.core.task.TaskData;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class SimulateRequestTest {
    protected static final Logger logger = LoggerFactory.getLogger(SimulateRequestTest.class);

    private static ZKInit zkInit;
    private static MockKernel kernel;
    private static MockKernel kernel_backup;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(true);

        // creating kernels for three nodes
        logger.info("creating kernel...");
        kernel = CoreSetup.createMockKernel(1);// new MockKernel(1);
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
        CoreSetup.tearDownZKInit(zkInit);
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        Gearman gearman = Gearman.createGearman();
        GearmanServer server = gearman.createGearmanServer(MockKernel.GEARMAN_SERVER_IP, 
                                                           MockKernel.GEARMAN_SERVER_PORT);
        String functionName = RequestCode.SIMULATE_REQUEST.gearmanFunctionName(); 

        SimulateRequestRegistrar registrar = new SimulateRequestRegistrar();
        Thread t = new Thread(registrar);
        t.start();
        while (!registrar.registrationComplete) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail("exception: " + e.getMessage());
            }
        }
        
        // submit a request job to gearman nettyserver
        GearmanClient client = gearman.createGearmanClient();
        client.addServer(server);
        String jsonString = "{\"request\":{\"stages\":3,\"sleep\":5000}}";
        
        GearmanJobReturn jobReturn = client.submitJob(functionName, jsonString.getBytes());
        while (!jobReturn.isEOF()) {
            GearmanJobEvent event = null;
            try {
                event = jobReturn.poll();
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail("exception: " + e.getMessage());
            }
            logger.info("event: {}", event.getEventType());
            byte[] data = event.getData();
            if (event.getEventType() == GearmanJobEventType.GEARMAN_JOB_SUCCESS
                    && data.length >= 8) {
                logger.info("data: {} [length: {}]", ByteUtil.readLong(data, 0), data.length);                
            } else {
                logger.info("data: {} [length: {}]", new String(data), data.length);
            }
        }
        
        try {
            TaskData task = DistTaskPool.defaultInstance().take(null);
            assertNotNull(task);
            logger.info("task id: {}, task data: {}", task.taskId, new String(task.taskData));
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        gearman.shutdown();
        registrar.shutdown();
    }

    
    private static class SimulateRequestRegistrar implements Runnable {
        private Gearman gearman = Gearman.createGearman();
        private GearmanWorker worker;
        private String functionName = RequestCode.SIMULATE_REQUEST.gearmanFunctionName(); 

        protected boolean registrationComplete = false;

        @Override
        public void run() {
            GearmanServer server = gearman.createGearmanServer(MockKernel.GEARMAN_SERVER_IP, 
                                                               MockKernel.GEARMAN_SERVER_PORT);
            worker = gearman.createGearmanWorker();
            worker.addFunction(functionName, new SimulateRequest());
            worker.addServer(server);
            logger.info("worker added to gearman nettyserver");
            registrationComplete = true;
        }
        
        public void shutdown() {
            if (worker != null) {
                worker.removeFunction(functionName);
            }
            if (gearman != null) {
                gearman.shutdown();
            }
        }
    }
}
