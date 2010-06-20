package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



/**
 * A topic data transfer object.
 */
public class Topic {

    public long id;
    public String typeId;
    public String label;
    public Map<String, Object> properties;

    public Topic(long id, String typeId, String label, Map properties) {
        this.id = id;
        this.typeId = typeId;
        this.label = label;
        this.properties = properties != null ? properties : new HashMap();
    }

    public Topic(Topic topic) {
        this(topic.id, topic.typeId, topic.label, topic.properties);
    }

    // ---

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    // ---

    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("id", id);
        o.put("type_id", typeId);
        o.put("label", label);
        o.put("properties", properties);
        return o;
    }

    @Override
    public String toString() {
        return "topic " + id + " (typeId=\"" + typeId + "\", label=\"" + label + "\")";
    }
}
