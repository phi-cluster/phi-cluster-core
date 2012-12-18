package org.phicluster.core;

public class Constants {

    public static final String ZN_TASK_ROOT = "/tasks";
    public static final String ZN_TASK_DATA = ZN_TASK_ROOT + "/data";
    public static final String ZN_TASK_QUEUE = ZN_TASK_ROOT + "/queue";
    public static final String ZN_TASK_TAKEN = ZN_TASK_ROOT + "/taken";
    public static final String ZN_TASK_SECONDARY = ZN_TASK_ROOT + "/secondary";
    
    public static final String ZN_NODES = "/nodes";
    public static final String ZN_HEARTBEAT = "/heartbeat";
    public static final String ZN_INBOX = "/inbox";

    public static final String ZN_JOBS_ROOT = "/jobs";
    public static final String ZN_JOB_ENTRY = ZN_JOBS_ROOT + "/entry";
    public static final String ZN_JOB_STATE = ZN_JOBS_ROOT + "/state";
    
    public static final String PREFIX_TASK = "task-";
    public static final String PREFIX_NODE = "node-";
    public static final String PREFIX_JOBS = "job-";
    public static final String SUFFIX_TASK_DONE = "done";

    // initial set of paths to be created during initialization
    // note that the paths are created in the order given below
    public static final String[] ZN_ARRAY = { ZN_TASK_ROOT, 
                                              ZN_TASK_DATA, 
                                              ZN_TASK_QUEUE,
                                              ZN_TASK_TAKEN, 
                                              ZN_TASK_SECONDARY, 
                                              ZN_NODES, 
                                              ZN_HEARTBEAT, 
                                              ZN_INBOX,
                                              ZN_JOBS_ROOT,
                                              ZN_JOB_ENTRY,
                                              ZN_JOB_STATE };
    
    public static final String ZN_TASK_DATA_PATH_WITH_PREFIX = 
            ZN_TASK_DATA + "/" + PREFIX_TASK;
    public static final String ZN_TASK_SECONDARY_WITH_PREFIX = 
            ZN_TASK_SECONDARY + "/" + PREFIX_NODE;

    public static final String ZN_JOB_ENTRY_WITH_PREFIX = 
            ZN_JOB_ENTRY + "/" + PREFIX_JOBS;
    public static final String ZN_JOB_STATE_WITH_PREFIX = 
            ZN_JOB_STATE + "/" + PREFIX_JOBS;
    
    public static final int REPLICATION_FACTOR = 3;
    public static final int HEARTBEAT_INTERVAL = 3000; // milliseconds
    
    // the number of misses to consider the node failed. 
    public static final int ACCRUAL_FAILURE_THRESHOLD = 3;
    
    public static final int MAX_CLUSTER_SIZE = 15;
    public static final int MIN_CLUSTER_SIZE = 3;
    
}
