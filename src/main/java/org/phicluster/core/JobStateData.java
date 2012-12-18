package org.phicluster.core;

import org.apache.zookeeper.data.Stat;

public class JobStateData {
    public final int jobId;
    public final byte[] data;
    public final Stat stat;
    
    public JobStateData(int jobId, byte[] data, Stat stat) {
        this.jobId = jobId;
        this.data = data;
        this.stat = stat;
    }
}