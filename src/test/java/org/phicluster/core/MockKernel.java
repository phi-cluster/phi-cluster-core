package org.phicluster.core;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.phicluster.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MockKernel extends Kernel {
//    public static final String ZOOKEEPER_FOR_UNIT_TEST = "localhost:2181";
    public static final String GEARMAN_SERVER_IP = "localhost";
    public static final int GEARMAN_SERVER_PORT = 4730;

    protected static final Logger logger = LoggerFactory.getLogger(MockKernel.class);

    public MockAccrualFailureDetector mockAccrualFailureDetector;
    protected DistTaskPool distTaskPool;
    
    protected List<String> paths = null;
        
    protected MockKernel(int nodeId) {
        super(nodeId,
              1,
              ConfigLoader.getInstance().getConfig().getZookeeperServer(),
              3000,
              null);
    }

    @Override
    public void process(WatchedEvent event) {
        if (kstate != KernelState.INITIALIZING) {
            logger.error("Invalid Kernel state: {}", kstate);
            return;
        }
        
        paths = createZookeeperNodes();
        //initHeartbeatModule();
        //initAccrualFailureDetectorModule();
        //initDistTaskPool();
        //initDistTaskExecutor();

        kstate = KernelState.INITIALIZED;
    }
    
    public void initDistTaskSystem() {
        mockAccrualFailureDetector = new MockAccrualFailureDetector(nodeId, zk, 0);
        distTaskPool = new DistTaskPool(nodeId, zk, mockAccrualFailureDetector);
        DistTaskPool.defaultInstance = distTaskPool;
        DistJobState djs = new DistJobState(zk);
        DistJobState.defaultInstance = djs;

    }
        
    public void tearDown() {
        logger.info("tearing down the kernel of node-{}, deleting zookeeper paths...", nodeId);
        if (paths != null) {
            for (String path : paths) {
                try {
                    deleteIfExists(path);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("exception while deleting path: {}: {}", path, e.getMessage());
                }
            }
        }
        logger.info("done - kernel of node-{}, zookeeper paths deleted...", nodeId);
    }
    
    protected void deleteIfExists(String path) throws Exception {
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            List<String> children = zk.getChildren(path, false);
            if (children != null && !children.isEmpty()) {
                for (String p : children) {
                    deleteIfExists(path + "/" + p);
                }
            }
            zk.delete(path, -1);
            logger.info("deleted: {}", path);
        } else {
            logger.info("does not exist: {}", path);
        }
    }
}
