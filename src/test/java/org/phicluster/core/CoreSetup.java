package org.phicluster.core;

import static org.junit.Assert.fail;

import org.apache.zookeeper.ZooKeeper.States;
import org.phicluster.core.Kernel;
import org.phicluster.core.ZKInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreSetup {
    protected static final Logger logger = LoggerFactory.getLogger(CoreSetup.class);
    
    public static ZKInit initZookeeper(String zookeeper, boolean clean) throws Exception {
        ZKInit zkInit = new ZKInit(zookeeper);
        boolean zkInitConnected = false;
        long timeout = 0l;
        while (timeout < 30*1000) {
            if (zkInit.zkState() == States.CONNECTED) {
                zkInitConnected = true;
                break;
            }
            timeout += 1000;
            Thread.sleep(1000);
        }
        if (!zkInitConnected) {
            fail("couldn't connect to zookeeper, zkInit: " + zkInit.zkState());
        }
        
        logger.info("connected to zookeeper, zkInit: {}", zkInit.zkState());
        
        if (clean) {
            // clean zookeeper 
            logger.info("cleaning zookeeper to ensure a clean start");
            zkInit.tearDown();
            logger.info("done - cleaning zookeeper to ensure a clean start\n");
        }
            
        // init zookeeper paths
        logger.info("creating zookeeper nodes for the cluster...");
        zkInit.init();
        logger.info("done - creating zookeeper nodes for the cluster...");

        return zkInit;
    }
    
    public static void tearDownZKInit(ZKInit zkInit) {
        zkInit.tearDown();
    }
    
    public static MockKernel createMockKernel(int nodeId) throws Exception {
        MockKernel kernel = new MockKernel(nodeId);
        kernel.init();
        
        boolean connected = false;
        long timeout = 0l;
        while (timeout < 30*1000) {
            if (kernel.kernelState() == Kernel.KernelState.INITIALIZED) {
                connected = true;
                break;
            }
            timeout += 1000;
            Thread.sleep(1000);
        }
        if (!connected) {
            fail("not connected: kernel- "+ nodeId + ": " + kernel.zk.getState());
        }
        
        logger.info("connected, kernel-{}: {}", nodeId, kernel.zk.getState());

        return kernel;
    }
}
