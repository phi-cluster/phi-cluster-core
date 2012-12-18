package org.phicluster.core;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.util.ByteUtil;


public enum NodeState {
    REQUESTED(1),
    BEING_PROVISIONED(2);
    
    // to save time for lookups
    private final static Map<Integer, NodeState> map = new HashMap<Integer, NodeState>();
    static {
        for (NodeState nodeState : NodeState.values()) {
            map.put(nodeState.state(), nodeState);
        }
    }
    
    private final int nodeState;
    
    private NodeState(int nodeState) {
        this.nodeState = nodeState;
    }
    
    public int state() {
        return nodeState;
    }
    
    public byte[] getBytes() {
        return ByteUtil.toBytes(nodeState);
    }
    
    public static NodeState state(int nodeState) {        
        return map.get(nodeState);
    }
    
}