package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



public class DataField {
    
    public String id;
    public String dataType;
    public String editor;
    public String indexMode;

    public DataField() {
        setDataType("text");
        setEditor("single line");
        setIndexMode("fulltext");
    }

    public DataField(String id) {
        this();
        setId(id);
    }

    public DataField(Map<String, String> properties) {
        setId(properties.get("id"));
        setDataType(properties.get("data_type"));
        setEditor(properties.get("editor"));
        setIndexMode(properties.get("index_mode"));
    }

    public DataField(JSONObject dataField) throws JSONException {
        this();
        setId(dataField.getString("id"));
        setDataType(dataField.getJSONObject("model").getString("type"));
        if (dataField.has("view")) {
            setEditor(dataField.getJSONObject("view").getString("editor"));
        }
        if (dataField.has("index_mode")) {
            setIndexMode(dataField.getString("index_mode"));
        }
    }

    // ---

    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap();
        properties.put("id", id);
        properties.put("data_type", dataType);
        properties.put("editor", editor);
        properties.put("index_mode", indexMode);
        return properties;
    }

    // ---

    public void setId(String id) {
        this.id = id;
    }

    // "text" (default) / "date" / "html" / "relation"
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    // "single line" (default) / "multi line"
    public void setEditor(String editor) {
        this.editor = editor;
    }

    // "fulltext" (default) / "id" / "off"
    public void setIndexMode(String indexMode) {
        this.indexMode = indexMode;
    }
}
