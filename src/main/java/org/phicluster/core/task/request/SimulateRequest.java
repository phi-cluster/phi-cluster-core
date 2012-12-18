package org.phicluster.core.task.request;

import org.gearman.GearmanFunctionCallback;
import org.phicluster.core.DistJobState;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.Job;
import org.phicluster.core.task.TaskCode;
import org.phicluster.core.task.TaskData;
import org.phicluster.core.task.Worker;
import org.phicluster.core.util.ByteUtil;
import org.phicluster.core.util.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimulateRequest extends Worker<RequestCode> {
    protected static final Logger logger = LoggerFactory.getLogger(SimulateRequest.class);
    
    protected final DistTaskPool distTaskPool;
    
    public SimulateRequest() {
        super(RequestCode.SIMULATE_REQUEST);
        this.distTaskPool = DistTaskPool.defaultInstance();
    }
    
    @Override
    public byte[] execute(String functionName, 
                          byte[] data,
                          GearmanFunctionCallback callback) throws Exception {
        
        // expected data: {request:{stages:3,sleep:5000}}
        String jsonString = new String(data);
        int stages = Parser.parseIntField(jsonString, "request.stages");
        int sleep = Parser.parseIntField(jsonString, "request.sleep");
        
        // create a job entry:
        DistJobState distJobState = DistJobState.defaultInstance();
        Job job = distJobState.createJobEntry(data);
        
        StringBuilder taskDataJson = new StringBuilder();
        taskDataJson.append("{\"task-data\":{\"task-code\":");
        taskDataJson.append(TaskCode.SIMULATE_TASK.code()).append(",");
        taskDataJson.append("\"jobId\":").append(job.jobId).append(",");
        taskDataJson.append("\"stages\":").append(stages).append(",");
        taskDataJson.append("\"sleep\":").append(sleep).append(",");
        taskDataJson.append("\"createdby\":").append(distTaskPool.nodeId());
        taskDataJson.append("}}");
        
        TaskData taskData = distTaskPool.offer(taskDataJson.toString());
        if (taskData == null) {
            logger.info("[node-{}] couldn't create task under job-{}",
                    new Object[] {distTaskPool.nodeId(), job.jobId});
            return ResponseCode.ERROR.getBytes();
        }
        logger.info("[node-{}] task-{} created under job-{}",
                new Object[] {distTaskPool.nodeId(), taskData.taskId, job.jobId});
                
        return ByteUtil.toBytes(taskData.taskId);
    }

}
