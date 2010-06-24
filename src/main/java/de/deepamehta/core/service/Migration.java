package de.deepamehta.core.service;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.service.Migration;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;



public abstract class Migration {
    
    protected DeepaMehtaService dms;

    public void setDeepaMehtaService(DeepaMehtaService dms) {
        this.dms = dms;
    }

    public abstract void run() throws Exception;

    // ---

    protected void readTypesFromFile(String file) throws Exception {
        InputStream is = getClass().getResourceAsStream(file);
        if (is == null) {
            throw new RuntimeException("Resource file \"" + file + "\" not found");
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuilder json = new StringBuilder();
        while ((line = in.readLine()) != null) {
            json.append(line);
        }
        createTypes(json.toString());
    }

    // ---

    private void createTypes(String json) throws JSONException {
        JSONArray types = new JSONArray(json);
        for (int i = 0; i < types.length(); i++) {
            TopicType topicType = new TopicType(types.getJSONObject(i));
            dms.createTopicType(topicType.properties, topicType.getDataFields());
        }
    }
}
