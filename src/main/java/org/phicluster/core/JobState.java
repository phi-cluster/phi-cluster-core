package org.phicluster.core;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.util.ByteUtil;


public enum JobState {
    INITIAL(1),
    CREATING_NODE_STATE_FIELDS(2);
    
    // to save time for lookups
    private final static Map<Integer, JobState> map = new HashMap<Integer, JobState>();
    static {
        for (JobState jobState : JobState.values()) {
            map.put(jobState.state(), jobState);
        }
    }
    
    private final int jobState;
    
    private JobState(int jobState) {
        this.jobState = jobState;
    }
    
    public int state() {
        return jobState;
    }
    
    public byte[] getBytes() {
        return ByteUtil.toBytes(jobState);
    }
    
    public static JobState state(int jobState) {        
        return map.get(jobState);
    }
    
}