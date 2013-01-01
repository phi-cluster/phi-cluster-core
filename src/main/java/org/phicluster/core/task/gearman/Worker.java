package org.phicluster.core.task.gearman;

import org.gearman.GearmanFunction;
import org.gearman.GearmanFunctionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Worker<T> implements GearmanFunction {
    protected static final Logger logger = LoggerFactory.getLogger(Worker.class);
    
    protected final T code;
    
    public Worker(T code) {
        this.code = code;
    }
    
    @Override
    public byte[] work(String functionName, 
                       byte[] data, 
                       GearmanFunctionCallback callback) throws Exception {
        try {
            return execute(functionName, data, callback);
        } catch (Exception e) {
            logger.error("Exception in worker [{}]", functionName);
            e.printStackTrace();
            throw e;
        }
    }

    
    public abstract byte[] execute(String functionName, 
                                   byte[] data, 
                                   GearmanFunctionCallback callback) throws Exception;
}
