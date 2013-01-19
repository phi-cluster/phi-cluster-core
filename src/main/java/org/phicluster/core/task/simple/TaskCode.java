package org.phicluster.core.task.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskCode {

    private static TaskCode ourInstance = new TaskCode();

    public static TaskCode getInstance() {
        return ourInstance;
    }

    private TaskCode() {
    }

    private Map<Integer, PhiRunnable> codes = new HashMap<>();

    public int addTaskCode(PhiRunnable runnable) {
        int taskCodeId = counter.getAndIncrement();
        codes.put(taskCodeId, runnable);
        return taskCodeId;
    }

    public PhiRunnable getTaskCode(int taskCodeId) {
        return codes.get(taskCodeId);
    }

    private AtomicInteger counter = new AtomicInteger();

}
