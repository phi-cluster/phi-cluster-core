package org.phicluster.core.util;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Parser {

    public static Integer parseIntField(String jsonString, String field) {
        Long v = (Long) parseForField(jsonString, field);
        return v.intValue();
    }
    
    public static String parseStringField(String jsonString, String field) {
        return (String) parseForField(jsonString, field);
    }
    
    /**
     * @param field is in format of "<field>.<field>..."
     */
    public static Object parseForField(String jsonString, String field) {
        JSONObject jobj = (JSONObject) JSONValue.parse(jsonString);
        String[] parts = field.split("\\.");
        for (int i = 0; i < parts.length-1; i++) {
            jobj = (JSONObject) jobj.get(parts[i]);
        }
        return jobj.get(parts[parts.length-1]);
    }
}
