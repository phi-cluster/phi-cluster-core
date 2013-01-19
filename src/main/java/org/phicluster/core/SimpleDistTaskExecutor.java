package org.phicluster.core;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.phicluster.core.task.PhiTask;
import org.phicluster.core.task.simple.PhiRunnable;
import org.phicluster.core.task.simple.TaskCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleDistTaskExecutor extends DistTaskExecutor {
    protected static final Logger logger = LoggerFactory.getLogger(SimpleDistTaskExecutor.class);

    private JSONParser parser = new JSONParser();
    private ExecutorService executorService;
    private TaskCode taskCode;

    protected SimpleDistTaskExecutor(DistTaskPool distTaskPool) {
        super(distTaskPool);
        executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors());
        taskCode = TaskCode.getInstance();

        logger.info("simpleTaskExecutor is created with {} threads",
                Runtime.getRuntime().availableProcessors());
    }

    @Override
    protected void executeTask(PhiTask task) {
        logger.info("a new task to execute: {}", task);
        String jsonTaskData = new String(task.taskData);
        Long tc;
        Long jobId;
        try {
            JSONObject parsed = (JSONObject) parser.parse(jsonTaskData);
            tc = (Long) parsed.get("task-code");
            jobId = (Long) parsed.get("job-id");
        } catch (ParseException e) {
            logger.warn("parsing failed: {}, not executing task: {}",
                    e.getMessage(), task.taskId);
            return;
        }
        logger.info("task-code: {}, job-id: {}", tc, jobId);
        PhiRunnable runnable = taskCode.getTaskCode(tc.intValue());
        runnable.setTaskData(task.taskData);
        executorService.submit(runnable);
        logger.info("task {} is submitted to executor", task.taskId);
    }

}
