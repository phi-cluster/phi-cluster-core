package org.phicluster.config;

public class Config {

    private String zookeeperServer;
    private int replicationFactor;

    private int httpServerPort;

    private int accrualFailureThreshold;
    private int accrualHeartbeatInterval;

    public String getZookeeperServer() {
        return zookeeperServer;
    }

    public void setZookeeperServer(String zookeeperServer) {
        this.zookeeperServer = zookeeperServer;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public int getHttpServerPort() {
        return httpServerPort;
    }

    public void setHttpServerPort(int httpServerPort) {
        this.httpServerPort = httpServerPort;
    }

    public int getAccrualFailureThreshold() {
        return accrualFailureThreshold;
    }

    public void setAccrualFailureThreshold(int accrualFailureThreshold) {
        this.accrualFailureThreshold = accrualFailureThreshold;
    }

    public int getAccrualHeartbeatInterval() {
        return accrualHeartbeatInterval;
    }

    public void setAccrualHeartbeatInterval(int accrualHeartbeatInterval) {
        this.accrualHeartbeatInterval = accrualHeartbeatInterval;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Config");
        sb.append("{zookeeperServer='").append(zookeeperServer).append('\'');
        sb.append(", replicationFactor=").append(replicationFactor);
        sb.append(", httpServerPort=").append(httpServerPort);
        sb.append(", accrualFailureThreshold=").append(accrualFailureThreshold);
        sb.append(", accrualHeartbeatInterval=").append(accrualHeartbeatInterval);
        sb.append('}');
        return sb.toString();
    }
}
