package org.phicluster.core.task.simple;

public interface PhiRunnable extends Runnable {

    public void setTaskData(byte[] data);

    public byte[] getTaskData();

}
