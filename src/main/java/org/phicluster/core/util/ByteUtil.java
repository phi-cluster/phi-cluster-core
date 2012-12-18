package org.phicluster.core.util;

public class ByteUtil {

    /**
     * Reads an integer from the byte array starting from the given offset.
     */
    public static int readInt(byte[] b, int offset) {
        if (b.length < offset + 4) {
            throw new ArrayIndexOutOfBoundsException("byte array has less than 4 bytes from offset: " + offset);
        }
        int v = (b[offset] & 0xFF) << 24;
        v += (b[offset + 1] & 0xFF) << 16;
        v += (b[offset + 2] & 0xFF) << 8;
        v += (b[offset + 3] & 0xFF) << 0;

        return v;
    }
    
    /**
     * Converts an integer into a 4-byte array. Most significant bit first.
     */
    public static byte[] toBytes(int v) {
        byte[] b = new byte[4];
        b[0] = (byte) ((v >>> 24) & 0xFF);
        b[1] = (byte) ((v >>> 16) & 0xFF);
        b[2] = (byte) ((v >>> 8) & 0xFF);
        b[3] = (byte) ((v >>> 0) & 0xFF);

        return b;
    }
    
    /**
     * Converts a long into a 8-byte array. Most significant bit first.
     */
    public static byte[] toBytes(long v) {
        byte[] b = new byte[8];
        b[0] = (byte) ((v >>> 56) & 0xFFL);
        b[1] = (byte) ((v >>> 48) & 0xFFL);
        b[2] = (byte) ((v >>> 40) & 0xFFL);
        b[3] = (byte) ((v >>> 32) & 0xFFL);
        b[4] = (byte) ((v >>> 24) & 0xFFL);
        b[5] = (byte) ((v >>> 16) & 0xFFL);
        b[6] = (byte) ((v >>> 8) & 0xFFL);
        b[7] = (byte) ((v >>> 0) & 0xFFL);

        return b;
    }
    
    /**
     * Reads a long from the byte array starting from the given offset.
     */
    public static long readLong(byte[] b, int offset) {
        if (b.length < offset + 8) {
            throw new ArrayIndexOutOfBoundsException("byte array has less than 8 bytes from offset: " + offset);
        }
        long v = (b[offset] & 0xFFL) << 56;
        v += (b[offset + 1] & 0xFFL) << 48;
        v += (b[offset + 2] & 0xFFL) << 40;
        v += (b[offset + 3] & 0xFFL) << 32;
        v += (b[offset + 4] & 0xFFL) << 24;
        v += (b[offset + 5] & 0xFFL) << 16;
        v += (b[offset + 6] & 0xFFL) << 8;
        v += (b[offset + 7] & 0xFFL) << 0;

        return v;
    }

}
