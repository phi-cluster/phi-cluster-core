package org.phicluster.core.task;

import org.apache.zookeeper.KeeperException;
import org.gearman.GearmanFunctionCallback;
import org.phicluster.core.DistJobState;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.JobStateData;
import org.phicluster.core.util.ByteUtil;
import org.phicluster.core.util.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SimulateTask extends Worker<TaskCode> {
    protected static final Logger logger = LoggerFactory.getLogger(SimulateTask.class);
    
    private String lcsFieldName = "last-completed-stage";
    
    public SimulateTask() {
        super(TaskCode.SIMULATE_TASK);
    }
    
    @Override
    public byte[] execute(String functionName, 
                          byte[] data,
                          GearmanFunctionCallback callback) throws Exception {
        // expected task: "<task-id>{task-data:{"task-code":1, "jobId":<id>}}"
        long taskId = ByteUtil.readLong(data, 0);
        String jsonString = new String(data, 8, data.length-8);
        
        int jobId = Parser.parseIntField(jsonString, "task-data.jobId");
        int numberOfStages = Parser.parseIntField(jsonString, "task-data.stages");
        int sleep = Parser.parseIntField(jsonString, "task-data.sleep");
        
        int executingNodeId = DistTaskPool.defaultInstance().nodeId();
        logger.info("[node-{}] running task-{} [job-{}]",
                new Object[] {executingNodeId, taskId, jobId});
        
        String fieldName = "executors";
        DistJobState distJobState = DistJobState.defaultInstance();
        
        try {
            byte[] initialData = String.valueOf(executingNodeId).getBytes();
            String path = distJobState.createJobStateField(jobId, fieldName, initialData);
            logger.info("path created by node-{}: {}", executingNodeId, path);
        } catch (KeeperException.NodeExistsException e) {
            JobStateData jsd = distJobState.readJobStateField(jobId, fieldName);
            String s = new String(jsd.data);
            s = s +"," + executingNodeId;
            distJobState.updateJobStateField(jobId, fieldName, s.getBytes(), jsd.stat.getVersion());
            logger.info("job state field '{}' updated with: {}", fieldName, s);
        }
        
        int lastCompletedStage = 0;
        try {
            JobStateData jsd = distJobState.readJobStateField(jobId, lcsFieldName);
            lastCompletedStage = ByteUtil.readInt(jsd.data, 0);
            logger.debug("[node-{}, job-{}] last completed stage: {}",
                    new Object[] {executingNodeId, jobId, lastCompletedStage});
        } catch (KeeperException.NoNodeException e) {
            // no stages completed before
            logger.debug("[node-{}, job-{}] last completed stage does not exist", executingNodeId, jobId);
        }

        for (int stage = lastCompletedStage + 1; stage < numberOfStages; stage++) {
            performStage(jobId, stage, sleep);
        }
        
        return ByteUtil.toBytes(taskId);
    }
        
    private void performStage(int jobId, int stage, int sleep) throws Exception {
        int executingNodeId = DistTaskPool.defaultInstance().nodeId();
        logger.info("[node-{}] performing stage-{} for job-{}", 
                new Object[] {executingNodeId, stage, jobId});
        
        // sleep for a while...
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // mark the stage complete
        DistJobState distJobState = DistJobState.defaultInstance();
        
        if (stage == 1) {
            // first stage: create path
            String path = distJobState.createJobStateField(jobId, lcsFieldName, ByteUtil.toBytes(1));
            logger.info("[node-{}] path created: {}", path);
            logger.info("[node-{}] performing stage-1 for job-{} done.", executingNodeId, jobId);
            return;
        }
        
        JobStateData jsd = distJobState.readJobStateField(jobId, lcsFieldName);
        int lastStage = ByteUtil.readInt(jsd.data, 0);
        if (lastStage == stage-1) {
            String msg = String.format("[node-%d, job-%d] unexpected last stage: %d, current stage: %d",
                    executingNodeId, jobId, lastStage, stage);
            logger.error(msg);
            throw new Exception(msg);
        }
        distJobState.updateJobStateField(jobId, lcsFieldName, ByteUtil.toBytes(stage), jsd.stat.getVersion());
        distJobState.createJobStateField(jobId, "stage-"+stage, ByteUtil.toBytes(executingNodeId));
    }
}
