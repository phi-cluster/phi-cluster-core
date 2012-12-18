package org.phicluster.core.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.phicluster.core.HeartbeatGeneratorTest;
import org.phicluster.core.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ByteUtilTest {
    protected static final Logger logger = LoggerFactory.getLogger(ByteUtilTest.class);

    @Test
    public void testReadInt() {
        byte[] b = new byte[] {0,0,0,1};
        int v = ByteUtil.readInt(b, 0);
        assertEquals(1, v);
        
        b = new byte[] {0,0,1,0};
        v = ByteUtil.readInt(b, 0);
        assertEquals(256, v);
        
        b = new byte[] {0,0,0,0};
        v = ByteUtil.readInt(b, 0);
        assertEquals(0, v);

        b = new byte[] {1,0,0,0,0,1};
        v = ByteUtil.readInt(b, 2);
        assertEquals(1, v);
        v = ByteUtil.readInt(b, 1);
        assertEquals(0, v);
    }

    @Test
    public void testToBytesInt() {
        byte[] expecteds = new byte[] {0,0,0,1};
        byte[] actuals = ByteUtil.toBytes(1);
        assertArrayEquals(expecteds, actuals);
        
        expecteds = new byte[] {0,0,1,0};
        actuals = ByteUtil.toBytes(256);
        assertArrayEquals(expecteds, actuals);

        expecteds = new byte[] {0,0,0,0};
        actuals = ByteUtil.toBytes(0);
        assertArrayEquals(expecteds, actuals);
        
        expecteds = new byte[] {0,1,0,0};
        actuals = ByteUtil.toBytes(65536);
        assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void testToBytesLong() {
        byte[] expecteds = new byte[] {0,0,0,0,0,0,0,1};
        byte[] actuals = ByteUtil.toBytes(1L);
        assertArrayEquals(expecteds, actuals);
        
        expecteds = new byte[] {0,0,0,0,0,0,1,0};
        actuals = ByteUtil.toBytes(256L);
        assertArrayEquals(expecteds, actuals);

        expecteds = new byte[] {0,0,0,0,0,0,0,0};
        actuals = ByteUtil.toBytes(0L);
        assertArrayEquals(expecteds, actuals);
        
        expecteds = new byte[] {0,0,0,0,0,1,0,0};
        actuals = ByteUtil.toBytes(65536L);
        assertArrayEquals(expecteds, actuals);
        
        long ts = System.currentTimeMillis();
        byte[] b = ByteUtil.toBytes(ts);
        long converted = ByteUtil.readLong(b, 0);
        logger.info("ts: {}, converted: {}", ts, converted);
        assertEquals(ts, converted);
    }

    @Test
    public void testReadLong() {
        byte[] b = new byte[] {0,0,0,0,0,0,0,1};
        long v = ByteUtil.readLong(b, 0);
        assertEquals(1, v);
        
        b = new byte[] {0,0,0,0,0,0,1,0};
        v = ByteUtil.readLong(b, 0);
        assertEquals(256, v);
        
        b = new byte[] {0,0,0,0,0,0,0,0};
        v = ByteUtil.readLong(b, 0);
        assertEquals(0, v);

        b = new byte[] {1,0,0,0,0,0,0,0,0,1};
        v = ByteUtil.readLong(b, 2);
        assertEquals(1, v);
        v = ByteUtil.readLong(b, 1);
        assertEquals(0, v);
    }

}
