package org.phicluster.core.task;

import java.nio.ByteBuffer;

import org.phicluster.core.util.ByteUtil;


public class TaskData {
    
    public final long taskId;
    public final byte[] taskData;
    
    public TaskData(long taskId, byte[] taskData) {
        this.taskId = taskId;
        this.taskData = taskData;
    }
    
    public byte[] taskDataWithTaskId() {
        ByteBuffer buffer = ByteBuffer.allocate(taskData.length+8);
        buffer.put(ByteUtil.toBytes(taskId));
        buffer.put(taskData);
        return buffer.array();
    }
}
