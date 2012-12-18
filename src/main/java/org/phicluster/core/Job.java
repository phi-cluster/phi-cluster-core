package org.phicluster.core;

public class Job {
    public final int jobId;
    public byte[] jobData;
    
    public Job(int jobId, byte[] jobData) {
        this.jobId = jobId;
        this.jobData = jobData;
    }
}
