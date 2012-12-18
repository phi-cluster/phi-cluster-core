package org.phicluster.core.task.request;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.task.Worker;
import org.phicluster.core.util.ByteUtil;


public enum RequestCode {
    SIMULATE_REQUEST(1, "simulateRequest", SimulateRequest.class);
    
    // to save time for lookups
    private final static Map<Integer, RequestCode> map = new HashMap<>();
    static {
        for (RequestCode requestCode : RequestCode.values()) {
            map.put(requestCode.code(), requestCode);
        }
    }
        
    private final int requestCode;
    private final String requestGearmanFunction;
    private final Class<? extends Worker<RequestCode>> gearmanWorkerClass;
    
    private RequestCode(int requestCode, String requestGearmanFunction, Class<? extends Worker<RequestCode>> gearmanWorkerClass) {
        this.requestCode = requestCode;
        this.requestGearmanFunction = requestGearmanFunction;
        this.gearmanWorkerClass = gearmanWorkerClass;
    }
    
    public int code() {
        return requestCode;
    }
    
    public String gearmanFunctionName() {
        return requestGearmanFunction;
    }
    
    public Worker<RequestCode> newWorkerInstance() throws InstantiationException, IllegalAccessException {
        return gearmanWorkerClass.newInstance();
    }
        
    public byte[] getBytes() {
        return ByteUtil.toBytes(requestCode);
    }
    
    public static RequestCode code(int requestCode) {        
        return map.get(requestCode);
    }
    
}