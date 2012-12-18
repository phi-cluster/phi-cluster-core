package org.phicluster.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.phicluster.core.AccrualFailureDetector;
import org.phicluster.core.Constants;

public class MockAccrualFailureDetector extends AccrualFailureDetector {
    
    public int numberOfNodes = 3;
    
    public MockAccrualFailureDetector(int nodeId, 
                                      ZooKeeper zk, 
                                      int interval) {
        super(nodeId, zk, interval);
    }

    public void run() {

    }

    @Override
    public Map<Integer, String> availableNodes() throws KeeperException, InterruptedException {
        
        Map<Integer, String> nodes = new HashMap<Integer, String>();
        for (int i = 2; i <= numberOfNodes; i++) { // skip node-1 (itself)
            nodes.put(i, Constants.PREFIX_NODE + i);                
        }
                    
        return nodes;            
    }
}