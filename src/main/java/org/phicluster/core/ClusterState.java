package org.phicluster.core;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.util.ByteUtil;


public enum ClusterState {
    WAITING_FOR_NODES(1),
    SETTING_UP_HADOOP(2),
    RUNNING_MR_JOB(3);
    
    // to save time for lookups
    private final static Map<Integer, ClusterState> map = new HashMap<Integer, ClusterState>();
    static {
        for (ClusterState clusterState : ClusterState.values()) {
            map.put(clusterState.state(), clusterState);
        }
    }
    
    private final int clusterState;
    
    private ClusterState(int clusterState) {
        this.clusterState = clusterState;
    }
    
    public int state() {
        return clusterState;
    }
    
    public byte[] getBytes() {
        return ByteUtil.toBytes(clusterState);
    }
    
    public static ClusterState state(int clusterState) {        
        return map.get(clusterState);
    }
    
}