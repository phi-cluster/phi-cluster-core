package org.phicluster.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.phicluster.core.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {
    protected static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    private XMLConfiguration xmlConfig;
    private Config config;

    private static ConfigLoader ourInstance = new ConfigLoader();

    public static ConfigLoader getInstance() {
        return ourInstance;
    }


    private ConfigLoader() {
        try {
            xmlConfig = new XMLConfiguration("phi-cluster.xml");
        } catch (ConfigurationException e) {
            logger.warn("configuration failed: {}", e);
        }

        build();

        logger.info("configuration loaded: {}", config);
    }

    private void build() {
        config = new Config ();

        config.setAccrualHeartbeatInterval(xmlConfig.getInt(
                "accrual-failure-detector.heartbeat-interval", Constants.HEARTBEAT_INTERVAL));
        config.setAccrualFailureThreshold(xmlConfig.getInt(
                "accrual-failure-detector.threshold", Constants.ACCRUAL_FAILURE_THRESHOLD));

        config.setHttpServerPort(xmlConfig.getInt("httpserver.port", 8080));

        config.setZookeeperServer(xmlConfig.getString("zookeeper.host", "localhost")
                + ':' + xmlConfig.getInt("zookeeper.port", 2181));
        config.setReplicationFactor(xmlConfig.getInt(
                "zookeeper.replication-factor", Constants.REPLICATION_FACTOR));
    }

    public Config getConfig() {
        return config;
    }
}
