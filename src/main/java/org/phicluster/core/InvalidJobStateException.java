package org.phicluster.core;

public class InvalidJobStateException extends Exception {

    private static final long serialVersionUID = 42567423462571L;
    
    public InvalidJobStateException(String msg) {
        super(msg);
    }

}
