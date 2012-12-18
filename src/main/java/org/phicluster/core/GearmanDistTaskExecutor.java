package org.phicluster.core;

import java.util.HashMap;
import java.util.Map;

import org.gearman.Gearman;
import org.gearman.GearmanClient;
import org.gearman.GearmanFunction;
import org.gearman.GearmanJobEvent;
import org.gearman.GearmanJobEventCallback;
import org.gearman.GearmanServer;
import org.gearman.GearmanWorker;
import org.phicluster.core.task.TaskCode;
import org.phicluster.core.task.TaskData;
import org.phicluster.core.task.request.RequestCode;
import org.phicluster.core.util.ByteUtil;
import org.phicluster.core.util.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class GearmanDistTaskExecutor extends DistTaskExecutor implements GearmanJobEventCallback<Integer> {
    protected static final Logger logger = LoggerFactory.getLogger(GearmanDistTaskExecutor.class);

    protected final Gearman gearman;
    protected final GearmanClient gearmanClient;
    protected final Map<String, GearmanServer> gearmanServers;
        
    public GearmanDistTaskExecutor(DistTaskPool distTaskPool, Map<String, Integer> gearmanServerHosts) {
        super(distTaskPool);
        
        this.gearman = Gearman.createGearman();
        this.gearmanClient = gearman.createGearmanClient();
        this.gearmanServers = new HashMap<String, GearmanServer>();
        
        for (String host : gearmanServerHosts.keySet()) {
            Integer port = gearmanServerHosts.get(host);
            GearmanServer gserver = createGearmanServer(host, port);
            gearmanServers.put(host, gserver);
            gearmanClient.addServer(gserver);
        }
        
        initGearmanWorkers();
    }

    private void initGearmanWorkers() {
        for (TaskCode tc : TaskCode.values()) {
            try {
                createWorkerForFunction(tc.gearmanFunctionName(), tc.newWorkerInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                logger.error("couldn't instantiate gearman worker for task: {}, exception: {}", 
                             tc, e.getMessage());
                e.printStackTrace();
            }
        }
        
        for (RequestCode rc : RequestCode.values()) {
            try {
                createWorkerForFunction(rc.gearmanFunctionName(), rc.newWorkerInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                logger.error("couldn't instantiate gearman worker for request: {}, exception: {}", 
                             rc, e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    protected GearmanServer createGearmanServer(String host, int port) {
        GearmanServer server = gearman.createGearmanServer(host, port); 
        logger.info("nettyserver reference created for {}:{}", host, port);
        
        return server;
    }
    
    protected GearmanWorker createWorkerForFunction(String functionName,
                                                    GearmanFunction function) {
        GearmanWorker worker = gearman.createGearmanWorker();
        addFunctionToWorker(worker, functionName, function);
        
        return worker;
    }
    
    protected void addFunctionToWorker(GearmanWorker worker,
                                       String functionName,
                                       GearmanFunction function) {
        worker.addFunction(functionName, function);
        
        for (GearmanServer server : gearmanServers.values()) {
            worker.addServer(server);
        }        
    }
    
    
    @Override
    protected void executeTask(TaskData task) {
        String jsonTaskData = new String(task.taskData);
        int tc = Parser.parseIntField(jsonTaskData, "task-data.task-code");
        TaskCode taskCode = TaskCode.code(tc);
        
        // submit job to gearman. TODO: consider background job?
        String gearmanFunctionName = taskCode.gearmanFunctionName();
        Integer callbackAttachment = taskCode.code();
        gearmanClient.submitJob(gearmanFunctionName, task.taskDataWithTaskId(), callbackAttachment, this);
    }    
        
    @Override
    public void onEvent(Integer taskCode, GearmanJobEvent event) {
        long taskId = ByteUtil.readLong(event.getData(), 0);
        logger.debug("[gearman job callback] taskId: {}, taskCode: {}, gearman job event: {}", 
                     new Object[] {taskId, TaskCode.code(taskCode), event.getEventType()});
    }

}
