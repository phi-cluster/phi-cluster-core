package org.phicluster.core;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.phicluster.config.Config;
import org.phicluster.config.ConfigLoader;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;



public class Kernel implements Watcher {
    protected static final Logger logger = LoggerFactory.getLogger(Kernel.class);
            
    protected final int nodeId;
    protected final int replicationFactor;
    protected final String zookeeperServers;
    protected final int sessionTimeout;
    protected final Config config = ConfigLoader.getInstance().getConfig();
    
    protected ZooKeeper zk;
    protected ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    
    protected AccrualFailureDetector accrualFailureDetector;
    protected HeartbeatGenerator heartbeadGenerator;
    protected DistTaskExecutor distTaskExecutor;
    protected DistTaskPool distTaskPool;

    public enum KernelState { INSTANTIATED, INITIALIZING, INITIALIZED, TERMINATED };
    protected KernelState kstate = null;
    
    protected Kernel(int nodeId, 
                     int replicationFactor, 
                     String zookeeperServers,
                     int sessionTimeout,
                     DistTaskExecutor distTaskExecutor) {
        this.nodeId = nodeId;
        this.replicationFactor = replicationFactor;
        this.zookeeperServers = zookeeperServers;
        this.sessionTimeout = sessionTimeout;
        this.distTaskExecutor = distTaskExecutor;
        
        this.kstate = KernelState.INSTANTIATED;
    }
    
    public void process(WatchedEvent event) {
        if (kstate != KernelState.INITIALIZING) {
            logger.error("Invalid Kernel state: {}", kstate);
            return;
        }
        
        createZookeeperNodes();
        initHeartbeatModule();
        initAccrualFailureDetectorModule();
        initDistTaskPool();
        initDistTaskExecutor();

        kstate = KernelState.INITIALIZED;
    }
    
    public void halt() {
        heartbeadGenerator.stopHeartbeats();
        accrualFailureDetector.stopAccrualFailureDetector();
        distTaskExecutor.stopExecutor();
    }
        
    protected void initHeartbeatModule() {
        String path = Constants.ZN_HEARTBEAT + "/" + Constants.PREFIX_NODE + nodeId;
        heartbeadGenerator = new HeartbeatGenerator(zk, path, config.getAccrualHeartbeatInterval());
        Thread t = new Thread(heartbeadGenerator);
        t.start();
        logger.info("heartbeat generator has started (path: {})", path);
    }
    
    protected void initAccrualFailureDetectorModule() {
        accrualFailureDetector = new AccrualFailureDetector(nodeId, zk, 10*1000);
        Thread t = new Thread(accrualFailureDetector);
        t.start();
    }
    
    protected void initDistTaskPool() {
        distTaskPool = new DistTaskPool(nodeId, zk, accrualFailureDetector);
        DistTaskPool.defaultInstance = distTaskPool;
    }
    
    protected void initDistTaskExecutor() {
        Thread t = new Thread(distTaskExecutor);
        t.start();
    }
        
    protected List<String> createZookeeperNodes() {
        String nodePath = Constants.ZN_NODES + "/" + Constants.PREFIX_NODE + nodeId;
        String heartbeatPath = Constants.ZN_HEARTBEAT + "/" + Constants.PREFIX_NODE + nodeId;
        String inboxPath = Constants.ZN_INBOX + "/" + Constants.PREFIX_NODE + nodeId;
        String secondaryTasksPath = Constants.ZN_TASK_SECONDARY + "/" + Constants.PREFIX_NODE + nodeId;

        List<String> znodesCreated = new ArrayList<String>();
        
        try {
            Stat znode = zk.exists(nodePath, false);
            long timestamp = System.currentTimeMillis();
            if (znode == null) {
                zk.create(nodePath, ByteUtil.toBytes(timestamp), acl, CreateMode.PERSISTENT);
            } else {
                zk.setData(nodePath, ByteUtil.toBytes(timestamp), -1);
            }
            znodesCreated.add(nodePath);
            
            znode = zk.exists(heartbeatPath, false);
            if (znode == null) {
                zk.create(heartbeatPath, ByteUtil.toBytes(timestamp), acl, CreateMode.PERSISTENT);
            } else {
                zk.setData(heartbeatPath, ByteUtil.toBytes(timestamp), -1);
            }
            znodesCreated.add(heartbeatPath);
            
            znode = zk.exists(inboxPath, false);
            if (znode == null) {
                zk.create(inboxPath, new byte[0], acl, CreateMode.PERSISTENT);
            } else {
                zk.setData(inboxPath, new byte[0], -1);
            }
            znodesCreated.add(inboxPath);
            
            znode = zk.exists(secondaryTasksPath, false);
            if (znode == null) {
                zk.create(secondaryTasksPath, ByteUtil.toBytes(timestamp), acl, CreateMode.PERSISTENT);
            } else {
                zk.setData(secondaryTasksPath, ByteUtil.toBytes(timestamp), -1);
            }            
            znodesCreated.add(secondaryTasksPath);
            
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }        
        
        return znodesCreated;
    }
            
    public KernelState kernelState() {
        return kstate;
    }
    
    public int nodeId() {
        return nodeId;
    }

    protected void init() throws Exception {
        kstate = KernelState.INITIALIZING;
        zk = new ZooKeeper(zookeeperServers, sessionTimeout, this);
    }
    
        
    public static void main(String[] args) {

    }


}
