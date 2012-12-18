package org.phicluster.core;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DistJobState {
    protected static final Logger logger = LoggerFactory.getLogger(DistJobState.class);
    
    protected final ZooKeeper zk;
    protected ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    protected static DistJobState defaultInstance;
    
    public DistJobState(ZooKeeper zk) {
        this.zk = zk;
    }

    public static DistJobState defaultInstance() {
        return defaultInstance;
    }
    
    public Job createJobEntry(byte[] jobData) throws KeeperException, InterruptedException {
        String jobEntryName = zk.create(Constants.ZN_JOB_ENTRY_WITH_PREFIX, 
                jobData, acl, CreateMode.PERSISTENT_SEQUENTIAL);

        String suffix = jobEntryName.substring(Constants.ZN_JOB_ENTRY_WITH_PREFIX.length());
        Integer jobId = new Integer(suffix);
        final Job job = new Job(jobId, jobData);
        
        // create znode for job state
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + job.jobId;
        byte[] data = JobState.INITIAL.getBytes();
        zk.create(path, data, acl, CreateMode.PERSISTENT);
        
        return job;
    }
        
    public Stat updateJobState(int jobId, byte[] state, int version) throws KeeperException, InterruptedException {
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + jobId;
        Stat stat = zk.setData(path, state, version);
        return stat;
    }
    
    public JobStateData readJobState(int jobId) throws KeeperException, InterruptedException {
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + jobId;
        Stat stat = new Stat();
        byte[] state = zk.getData(path, false, stat);
        final JobStateData jobState = new JobStateData(jobId, state, stat);
        return jobState;
    }
    
    public String createJobStateField(int jobId, String fieldName, byte[] initialData) throws KeeperException, InterruptedException {
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + jobId + "/" + fieldName;
        String p = zk.create(path, initialData, acl, CreateMode.PERSISTENT);
        return p;
    }
    
    public Stat updateJobStateField(int jobId, String fieldName, byte[] data, int version) throws KeeperException, InterruptedException {
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + jobId + "/" + fieldName;
        Stat stat = zk.setData(path, data, version);
        return stat;
    }
    
    public JobStateData readJobStateField(int jobId, String fieldName) throws KeeperException, InterruptedException {
        String path = Constants.ZN_JOB_STATE_WITH_PREFIX + jobId + "/" + fieldName;
        Stat stat = new Stat();
        byte[] data = zk.getData(path, false, stat);
        final JobStateData jobData = new JobStateData(jobId, data, stat);
        return jobData;        
    }
}
