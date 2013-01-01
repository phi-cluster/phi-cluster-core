package org.phicluster.core;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.phicluster.core.task.PhiTask;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DistTaskPool {
    protected static final Logger logger = LoggerFactory.getLogger(DistTaskPool.class);
    
    protected final int nodeId;
    protected final ZooKeeper zk;
    protected ArrayList<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    protected final AccrualFailureDetector failureDetector;
    
    protected static DistTaskPool defaultInstance;
    
    public DistTaskPool(int nodeId, ZooKeeper zk, AccrualFailureDetector failureDetector) {
        this.nodeId = nodeId;
        this.zk = zk;
        
        this.failureDetector = failureDetector;
    }
    
    public static DistTaskPool defaultInstance() {
        return defaultInstance;
    }
    
    public int nodeId() {
        return this.nodeId;
    }
    
    public PhiTask take(Watcher watcher) throws Exception {
        List<String> children = zk.getChildren(Constants.ZN_TASK_QUEUE, watcher);
        if (children.isEmpty()) {
            return null; // no tasks in the queue
        }
        
        // sort tasks by task id
        TreeMap<Long, String> taskMap = new TreeMap<Long, String>();
        for (String child : children) {
            String suffix = child.substring(Constants.PREFIX_TASK.length());
            Long taskId = new Long(suffix);
            taskMap.put(taskId, child);
        }
        
        // go through the list until succeeding in taking a task
        for (Long taskId : taskMap.keySet()) {
            String taskTakenPath = Constants.ZN_TASK_TAKEN + "/" + Constants.PREFIX_TASK + taskId;
            Stat znode = zk.exists(taskTakenPath, null);
            if (znode != null) {
                // task is already taken, skip it
                continue;
            }
            
            // first, put the task into other nodes' secondary task lists 
            // in case there is failure before the completion of the task
            List<String> secondaryTaskPaths = replicate(taskId);
            if (secondaryTaskPaths == null || secondaryTaskPaths.isEmpty()) {
                // couldn't replicate the task, skip it
                logger.warn("couldn't replicate task {} into secondary task " +
                		"lists of other nodes, giving up this task.", taskId);
                continue;
            }
            
            // second, create a znode in "taken" tasks list
            try {
                zk.create(taskTakenPath, ByteUtil.toBytes(nodeId), acl, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ne) {
                logger.debug("couldn't create the task {} in 'taken' list," +
                		"task must have already been taken", taskId);
                // delete the tasks created in secondary task lists
                deleteZNodes(secondaryTaskPaths);
                continue;
            } catch (Exception e) {
                //e.printStackTrace();
                logger.error("exception in creating task {} in 'taken' tasks list: {}",
                        taskId, e);
                // delete the tasks created in secondary task lists
                deleteZNodes(secondaryTaskPaths);
                throw e;
            }

            // finally, delete the task from the queue
            String taskQueuePath = Constants.ZN_TASK_QUEUE + "/" + taskMap.get(taskId);
            zk.delete(taskQueuePath, -1);

            // the task data
            String taskDataPath = Constants.ZN_TASK_DATA_PATH_WITH_PREFIX + String.format("%010d", taskId);
            byte[] taskData = zk.getData(taskDataPath, false, null);
            return new PhiTask(taskId, taskData);
        }
        
        return null;
    }
    
    public void markTaskDone(long taskId) throws Exception {
        // first check the owner
        String taskTakenPath = Constants.ZN_TASK_TAKEN + "/" + Constants.PREFIX_TASK + taskId;
        try {
            byte[] data = zk.getData(taskTakenPath, false, null);
            int ownerId = ByteUtil.readInt(data, 0);
            if (ownerId != this.nodeId) {
                logger.error("task owner does not match; owner's node id: {}", ownerId);
                throw new Exception("task owner does not match; owner's node id: " + ownerId);
            }
        } catch (KeeperException.NoNodeException e) {
            logger.error("no path for task {} in taken list: {}", taskId, taskTakenPath);
            throw e;
        } catch (KeeperException | InterruptedException e) {
            throw e;
        }
        
        StringBuilder path = new StringBuilder();
        path.append(Constants.ZN_TASK_DATA_PATH_WITH_PREFIX);
        path.append(String.format("%010d", taskId));
        path.append('/');
        path.append(Constants.SUFFIX_TASK_DONE);
        
        String p = zk.create(path.toString(), ByteUtil.toBytes(nodeId), acl, CreateMode.PERSISTENT);
        logger.debug("path (znode) created to mark task complete: {}", p);
    }
    
    protected void deleteZNodes(List<String> paths) throws InterruptedException, KeeperException {
        for (String path : paths) {
            zk.delete(path, -1);
        }
    }
    
    protected List<String> replicate(Long taskId) throws KeeperException, InterruptedException {
        Set<String> replicationNodes = getReplicationNodes();
        
        List<String> pathsCreated = new ArrayList<String>();
        for (String nodeName : replicationNodes) {
            String secondaryTaskPath = Constants.ZN_TASK_SECONDARY 
                    + "/" + nodeName + "/" + Constants.PREFIX_TASK + taskId;
            
            try {
                zk.create(secondaryTaskPath, ByteUtil.toBytes(this.nodeId), acl, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // node already exists, roll-back previously created nodes.
                logger.debug("secondary task path exists: {}", secondaryTaskPath);
                for (String p : pathsCreated) {
                    zk.delete(p, -1);
                }
                return null;
            }
            
            pathsCreated.add(secondaryTaskPath);
        }
        
        return pathsCreated;
    }
    
    protected Set<String> getReplicationNodes() throws KeeperException, InterruptedException {
        Map<Integer, String> nodes = failureDetector.availableNodes();
        Set<String> nodesSelected = new HashSet<String>();
        
        if (nodes.size() < Constants.REPLICATION_FACTOR) {
            nodesSelected.addAll(nodes.values());
            if (nodes.size() < Constants.REPLICATION_FACTOR-1) {
                logger.warn("# of nodes ({}) is less than replication factor: {}", 
                        nodes.size()+1, Constants.REPLICATION_FACTOR);
            }
            
            return nodesSelected;
        }
        
        // randomly select nodes
        String[] nodePaths = nodes.values().toArray(new String[0]);
        while (nodesSelected.size() < Constants.REPLICATION_FACTOR-1) {
            int index = (int) Math.floor(Math.random() * (double)nodePaths.length);
            nodesSelected.add(nodePaths[index]);
        }
        
        return nodesSelected;
    }
    
    
    public PhiTask offer(String jsonTaskData) throws Exception {
        byte[] taskData = jsonTaskData.getBytes();
        String taskName = zk.create(Constants.ZN_TASK_DATA_PATH_WITH_PREFIX, 
                taskData, acl, CreateMode.PERSISTENT_SEQUENTIAL);
        String suffix = taskName.substring(Constants.ZN_TASK_DATA_PATH_WITH_PREFIX.length());
        Long taskId = new Long(suffix);
        PhiTask task = new PhiTask(taskId, taskData);
        
        // create task in the queue
        createTaskInQueue(taskId);
        
        return task;
    }
    
    protected void createTaskInQueue(long taskId) throws KeeperException, InterruptedException {
        // create task in the queue
        String taskQueuePath = Constants.ZN_TASK_QUEUE + "/" + Constants.PREFIX_TASK + taskId;
        zk.create(taskQueuePath, new byte[0], acl, CreateMode.PERSISTENT);
    }
}
