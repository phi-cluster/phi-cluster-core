package org.phicluster.core.task;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.util.ByteUtil;


public enum TaskCode {
    SIMULATE_TASK(1,"simulateTask", SimulateTask.class); // experimental task
    
    // to save time for lookups
    private final static Map<Integer, TaskCode> map = new HashMap<>();
    static {
        for (TaskCode taskCode : TaskCode.values()) {
            map.put(taskCode.code(), taskCode);
        }
    }
            
    private final int taskCode;
    private final String taskGearmanFunction;
    private final Class<? extends Worker<TaskCode>> gearmanWorkerClass;
    
    private TaskCode(int taskCode, String taskGearmanFunction, Class<? extends Worker<TaskCode>> gearmanWorkerClass) {
        this.taskCode = taskCode;
        this.taskGearmanFunction = taskGearmanFunction;
        this.gearmanWorkerClass = gearmanWorkerClass;
    }
    
    public int code() {
        return taskCode;
    }
    
    public String gearmanFunctionName() {
        return taskGearmanFunction;
    }
    
    public Worker<TaskCode> newWorkerInstance() throws InstantiationException, IllegalAccessException {
        return gearmanWorkerClass.newInstance();
    }
        
    public byte[] getBytes() {
        return ByteUtil.toBytes(taskCode);
    }
    
    public static TaskCode code(int taskCode) {        
        return map.get(taskCode);
    }
    
}