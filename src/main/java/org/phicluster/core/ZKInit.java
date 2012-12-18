package org.phicluster.core;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKInit implements Watcher {
    protected static final Logger logger = LoggerFactory.getLogger(ZKInit.class);
            
    protected ZooKeeper zk = null;
    protected ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    
    protected Stat root = null;
        
    public ZKInit(String zookeeperServers) throws Exception {
        this.zk = new ZooKeeper(zookeeperServers, 3000, this);
    }

    public void process(WatchedEvent event) {
        logger.info("notification received: {}", event);
        try {
            root = zk.exists("/", false);
            logger.info("root: {}", root);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
    }
    
    protected States zkState() {
        return zk.getState();
    }
    
    protected void init() {
        if (zk.getState() != States.CONNECTED) {
            throw new RuntimeException("not connected to zookeeper: " + zk.getState());
        }
        for (String path : Constants.ZN_ARRAY) {
            try {
                Stat znode = zk.exists(path, false); 
                if (znode != null) {
                    logger.warn("znode exists: {}", znode);
                    continue;
                }
                zk.create(path, new byte[0], acl, CreateMode.PERSISTENT);
                znode = zk.exists(path, false);
                logger.info("znode created: {}", znode);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    protected void tearDown() {
        for (String path : Constants.ZN_ARRAY) {
            try {
                deleteIfExists(path);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("exception while deleting path: {}: {}", path, e.getMessage());
            }
        }
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
    
    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("log4j.properties");
        //ZKInit zkinit = new ZKInit("15.185.228.21:2181,15.185.228.8:2181,15.185.227.26:2181");
        ZKInit zkinit = new ZKInit("15.185.228.137:2181");
        while (zkinit.zkState() != States.CONNECTED) {
            System.out.print('.');
            Thread.sleep(1000);
        }
        System.out.println("\nzookeeper state: " + zkinit.zkState());
        //System.out.println("creating paths:");
        //zkinit.init();
        zkinit.tearDown();
        System.out.println("done");
    }
}
