package org.phicluster.core.task.simple;

import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.phicluster.core.DistJobState;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTask implements PhiRunnable {
    protected static final Logger logger = LoggerFactory.getLogger(SimpleTask.class);

    private byte[] data;

    @Override
    public void run() {
        logger.info("starting simple task...");
        if (data == null) {
            logger.error("taskData is null!");
        }
        String jsonTaskData = new String(data);
        JSONParser parser = new JSONParser();
        Long tc;
        Long jobId;
        try {
            JSONObject parsed = (JSONObject) parser.parse(jsonTaskData);
            tc = (Long) parsed.get("task-code");
            jobId = (Long) parsed.get("job-id");
        } catch (ParseException e) {
            logger.warn("parsing failed: {}", e.getMessage());
            return;
        }

        int executingNodeId = DistTaskPool.defaultInstance().nodeId();
        logger.info("[node-{}] running task-{} [job-{}]",
                new Object[] {executingNodeId, this, jobId});

        String fieldName = "executors";
        DistJobState distJobState = DistJobState.defaultInstance();

        Job job = null;
        try {
            job = distJobState.createJobEntry("{\"job-id\":1}".getBytes());
        } catch (KeeperException e) {
            e.printStackTrace();
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }

        logger.info("job created: {}", job);
    }

    @Override
    public void setTaskData(byte[] data) {
        this.data = data;
    }

    @Override
    public byte[] getTaskData() {
        return data;
    }

}
