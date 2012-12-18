package org.phicluster.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.phicluster.core.HeartbeatGenerator;
import org.phicluster.core.ZKInit;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HeartbeatGeneratorTest {
    protected static final Logger logger = LoggerFactory.getLogger(HeartbeatGeneratorTest.class);
    
    private static ZKInit zkInit;
    private static MockKernel kernel;

    private String path = "/heartbeat/node-1";
    private int interval = 5000;
    private HeartbeatGenerator hg;

    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkInit = CoreSetup.initZookeeper(MockKernel.ZOOKEEPER_FOR_UNIT_TEST, true);

        logger.info("creating kernel...");
        kernel = CoreSetup.createMockKernel(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("tearing down unit test....");
        
        // clean kernel data in zk
        kernel.tearDown();
        
        // clean cluster data in zk
        zkInit.tearDown();
        logger.info("done.\n");
    }

    @Before
    public void setUp() throws Exception {
        hg = new HeartbeatGenerator(kernel.zk, path, interval);
        Thread t = new Thread(hg);
        t.start();
        int timeout = 0;
        while (timeout < 3) {
            if (hg.state == HeartbeatGenerator.State.RUNNING) {
                break;
            }
            Thread.sleep(1000);
            timeout++;
        }
        assertEquals(HeartbeatGenerator.State.RUNNING, hg.state);
    }

    @After
    public void tearDown() throws Exception {
        hg.stopHeartbeats();
        int timeout = 0;
        while (timeout++ < 5) {
            if (hg.state == HeartbeatGenerator.State.STOPPED) {
                break;
            }
            Thread.sleep(1000);
        }
        assertEquals(HeartbeatGenerator.State.STOPPED, hg.state);
    }

    @Test
    public void testRun() {
        logger.info("running heartbeat generator...");
        
        List<Long> hearbeats = new ArrayList<Long>();
        for (int i = 0; i < 4; i++) {
            try {
                Thread.sleep(interval+500);
                byte[] b = kernel.zk.getData(path, false, null);
                long timestamp = ByteUtil.readLong(b, 0);
                hearbeats.add(timestamp);
                logger.info("heartbeat timestamp read: {}", timestamp);
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
                fail("exception: " + e.getMessage());
            };
        }

        for (int i = 1; i < 4; i++) {
            long ts1 = hearbeats.get(i-1);
            long ts2 = hearbeats.get(i);
            long diff = ts2 - ts1;
            assertTrue("two consecutive heartbeats are too far away: " + diff, diff < (interval * 2));
        }
        logger.info("done.");
    }

    @Test
    public void testSuspendAndResume() {
        logger.info("testing suspending/resuming heartbeat generator...");
        try {
            // make sure heartbeat generator works
            Thread.sleep(2000); // give a little time for the first heartbeat
            byte[] b = kernel.zk.getData(path, false, null);
            long ts1 = ByteUtil.readLong(b, 0);
            logger.info("ts1: {}", ts1);
            Thread.sleep(interval + 1000);
            b = kernel.zk.getData(path, false, null);
            long ts2 = ByteUtil.readLong(b, 0);
            logger.info("ts2: {}", ts2);
            long diff = ts2 - ts1;
            assertTrue("two consecutive heartbeats are too far away: " + diff, diff < (interval * 2));
            
            // now suspend heartbeat generator 
            hg.suspend();
            
            // make sure it suspends
            int timeout = 0;
            while (hg.state != HeartbeatGenerator.State.SUSPENDED) {
                Thread.sleep(1000);
                assertTrue("couldn't suspend heartbeat generator in " + timeout + " seconds, state: " + hg.state, 
                           timeout < 10);
            }
            logger.info("heartbeat generator suspended, state: {}", hg.state);
            
            
            // read the current timestamp
            b = kernel.zk.getData(path, false, null);
            ts1 = ByteUtil.readLong(b, 0);
            logger.info("last heartbeat timestamp: {}", ts1);
            
            // wait for a while
            logger.info("waiting {} seconds before reading again", ((interval*2)/1000));
            Thread.sleep(interval*2);
            
            // read the timestamp again
            b = kernel.zk.getData(path, false, null);
            ts2 = ByteUtil.readLong(b, 0);
            logger.info("reading heartbeat timestamp again: {}", ts2);
            
            // timestamp must not change
            assertEquals("heartbeat timestamp has been changed while suspended", ts1, ts2);
            
            // resume
            hg.resume();
            logger.info("resuming heartbeat generator");
            
            // make sure it suspends
            timeout = 0;
            while (hg.state != HeartbeatGenerator.State.RUNNING) {
                Thread.sleep(1000);
                assertTrue("couldn't resume heartbeat generator in " + timeout + " seconds, state: " + hg.state, 
                           timeout < 10);
            }
            logger.info("heartbeat generator has been resumed, state: {}", hg.state);

            
            // wait for a while
            logger.info("waiting {} seconds before reading timestamp", (interval/1000));
            Thread.sleep(interval);
            
            // read the timestamp again, it must be updated
            b = kernel.zk.getData(path, false, null);
            ts2 = ByteUtil.readLong(b, 0);
            logger.info("latest timestamp read after resuming: {}", ts2);
            
            assertTrue("timestamp must be updated after resuming the generator", ts2 > ts1);
            
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            fail("exception: " + e.getMessage());
        }
        
        logger.info("done - test suspend-resume");
    }
    
}
