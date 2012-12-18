package org.phicluster.core.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.phicluster.core.util.Parser;

public class ParserTest {

    @Test
    public void testParseIntField() {
        String jsonString = "{\"request\":{\"task-code\":1, \"instances\":3, \"jar-path\":\"/to/mr-jar/mr.jar\"}}";
        String field = "request.task-code";
        int actual = Parser.parseIntField(jsonString, field);
        assertEquals(1, actual);
    }

    
    @Test
    public void testParseStringField() {
        String jsonString = "{\"request\":{\"task-code\":1, \"instances\":3, \"jar-path\":\"/to/mr-jar/mr.jar\"}}";
        String field = "request.jar-path";
        String actual = Parser.parseStringField(jsonString, field);
        assertEquals("/to/mr-jar/mr.jar", actual);
    }
}
