package org.phicluster.core.task.request;

import java.util.HashMap;
import java.util.Map;

import org.phicluster.core.util.ByteUtil;


public enum ResponseCode {
    SUCCESS(200),
    ERROR(400);
    
    private final static Map<Integer, ResponseCode> map = new HashMap<>();
    static {
        for (ResponseCode responseCode : ResponseCode.values()) {
            map.put(responseCode.code(), responseCode);
        }
    }
    
    private final int responseCode;
    
    private ResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }
    
    public int code() {
        return responseCode;
    }
    
    public byte[] getBytes() {
        return ByteUtil.toBytes(responseCode);
    }
    
    public static ResponseCode code(int responseCode) {
        return map.get(responseCode);
    }
}
