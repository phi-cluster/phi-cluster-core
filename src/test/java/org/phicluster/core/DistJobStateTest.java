package org.phicluster.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.*;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class DistJobStateTest {
    protected static final Logger logger = LoggerFactory.getLogger(DistJobStateTest.class);

    private static ZKInit zkInit;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(true);
        // create the default instance
        DistJobState djs = new DistJobState(zkInit.zk);
        DistJobState.defaultInstance = djs;
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("tearing down unit test....");

        // clean cluster data in zk
        zkInit.tearDown();
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateJobEntry() {
        logger.info("test case - testCreateJobEntry");

        // create a few job entries 
        int numberOfJobs = 5;
        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < numberOfJobs; i++) {
            String jobData = "unittest-jobs-" + i;
            try {
                Job job = DistJobState.defaultInstance().createJobEntry(jobData.getBytes());
                assertNotNull(job);
                jobs.add(job);
                logger.info("job created: {}", job.jobId);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                fail("exception: " + e.getMessage());
            }
        }
        
        // make sure job ids are in increasing order
        for (int i = 1; i < numberOfJobs; i++) {
            Job job1 = jobs.get(i-1);
            Job job2 = jobs.get(i);
            assertTrue("job ids were expected to increase", job1.jobId < job2.jobId);
        }
        
        // check job state znode 
        for (int i = 0; i < numberOfJobs; i++) {
            Job job = jobs.get(i);
            String p = "/jobs/state/job-" + job.jobId;
            try {
                Stat stat = zkInit.zk.exists(p, false);
                assertNotNull("job state path does not exist: " + p, stat);
                logger.info("job state exists: {}", p);
                byte[] data = zkInit.zk.getData(p, false, stat);
                JobState state = JobState.state(ByteUtil.readInt(data, 0));
                assertEquals(JobState.INITIAL, state);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                fail("exception: " + e.getMessage());
            }
        }
        
        logger.info("done.");
    }
    
    @Test
    public void testUpdateJobState() {
        String jobData = "unittest-job-for-update-state";
        try {
            Job job = DistJobState.defaultInstance().createJobEntry(jobData.getBytes());
            assertNotNull(job);
            logger.info("job created: {}", job.jobId);
            
            // read current version
            String p = "/jobs/state/job-" + job.jobId;
            Stat stat = zkInit.zk.exists(p, false);
            assertNotNull(stat);
            
            // update the state
            String updatedState = "updated state";
            int version = stat.getVersion();
            zkInit.zk.setData(p, updatedState.getBytes(), version);
            
            // read it again to confirm the update
            byte[] data = zkInit.zk.getData(p, false, stat);
            String stateRead = new String(data);
            assertEquals("updated state is not what is expected", updatedState, stateRead);
            assertTrue(stat.getVersion() > version);
            
            // try to change with out-dated version, which means the data has changed 
            try {
                zkInit.zk.setData(p, updatedState.getBytes(), version);
                fail("bad version exception was expected");
            } catch (KeeperException.BadVersionException bve) {
                // this is expected
                logger.info("as expected, version does not match: {}", version);
            }
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
    }
    
    @Test
    public void testReadJobState() {
        logger.info("testReadJobState");

        String jobData = "unittest-job-for-update-state";
        try {
            Job job = DistJobState.defaultInstance().createJobEntry(jobData.getBytes());
            assertNotNull(job);
            logger.info("job created: {}", job.jobId);
        
            // read current version
            String p = "/jobs/state/job-" + job.jobId;
            Stat stat = zkInit.zk.exists(p, false);
            assertNotNull(stat);

            JobStateData jsd = DistJobState.defaultInstance().readJobState(job.jobId);
            assertArrayEquals("unexpected job state for: " + p, 
                              JobState.INITIAL.getBytes(), jsd.data);
            logger.info("initial job state read for: {}", p);
            
            // change job state and read it back
            byte[] data = "new state".getBytes();
            zkInit.zk.setData(p, data, jsd.stat.getVersion());
            logger.info("data set for: {}", p);
            
            jsd = DistJobState.defaultInstance().readJobState(job.jobId);
            assertArrayEquals("unexpected job state for: " + p, data, jsd.data);
            logger.info("job state read for: {}", p);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }
    
    @Test
    public void testCreateJobStateField() {
        logger.info("testCreateJobStateField");

        DistJobState distJobState = DistJobState.defaultInstance();
        String jobData = "unittest-job-for-update-state";
        try {
            Job job = distJobState.createJobEntry(jobData.getBytes());
            assertNotNull(job);
            logger.info("job created: {}", job.jobId);

            String fieldName = "unittest-field";
            byte[] data = "initial value".getBytes();
            String path = distJobState.createJobStateField(job.jobId, fieldName, data);
            String expectedPath = "/jobs/state/job-" + job.jobId + "/" + fieldName;
            assertEquals(expectedPath, path);
            
            byte[] fieldData = zkInit.zk.getData(path, false, null);
            assertArrayEquals(data, fieldData);
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }
    
    @Test
    public void testUpdateJobStateField() {
        logger.info("testUpdateJobStateField");

        DistJobState distJobState = DistJobState.defaultInstance();
        String jobData = "unittest-job-for-update-state";
        try {
            Job job = distJobState.createJobEntry(jobData.getBytes());
            assertNotNull(job);
            logger.info("job created: {}", job.jobId);

            String fieldName = "unittest-field";
            byte[] data = "initial value".getBytes();
            String path = distJobState.createJobStateField(job.jobId, fieldName, data);
            logger.info("job state field created: {}", path);

            // read the current version
            Stat stat = zkInit.zk.exists(path, false);

            // update the field data
            byte[] updateData = "updated value".getBytes();
            distJobState.updateJobStateField(job.jobId, fieldName, updateData, stat.getVersion());

            // read the field data to compare
            byte[] actual = zkInit.zk.getData(path, false, stat);
            assertArrayEquals(updateData, actual);
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }
    
    @Test
    public void testReadJobStateField() {
        logger.info("testUpdateJobStateField");

        DistJobState distJobState = DistJobState.defaultInstance();
        String jobData = "unittest-job-for-update-state";
        try {
            Job job = distJobState.createJobEntry(jobData.getBytes());
            assertNotNull(job);
            logger.info("job created: {}", job.jobId);

            String fieldName = "unittest-field";
            byte[] data = "initial value".getBytes();
            String path = distJobState.createJobStateField(job.jobId, fieldName, data);
            logger.info("job state field created: {}", path);
        
            JobStateData jsd = distJobState.readJobStateField(job.jobId, fieldName);
            assertArrayEquals(data, jsd.data);

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done.");
    }

}
