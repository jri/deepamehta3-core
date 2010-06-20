package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



public class DataField {
    
    public String id;
    public String dataType;
    public String relatedTypeId;    // used for dataType="relation" fields
    public String editor;
    public String indexingMode;

    public DataField() {
        setDataType("text");
        setEditor("single line");
        setIndexingMode("OFF");
    }

    public DataField(String id) {
        this();
        setId(id);
    }

    public DataField(Map<String, String> properties) {
        setId(properties.get("id"));
        setDataType(properties.get("data_type"));
        setRelatedTypeId(properties.get("related_type_id"));
        setEditor(properties.get("editor"));
        setIndexingMode(properties.get("indexing_mode"));
    }

    public DataField(JSONObject dataField) throws JSONException {
        this();
        setId(dataField.getString("id"));
        setDataType(dataField.getJSONObject("model").getString("type"));
        if (dataType.equals("relation")) {
            setRelatedTypeId(dataField.getJSONObject("model").getString("related_type_id"));
        }
        if (dataField.has("view")) {
            setEditor(dataField.getJSONObject("view").getString("editor"));
        }
        if (dataField.has("indexing_mode")) {
            setIndexingMode(dataField.getString("indexing_mode"));
        }
    }

    // ---

    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("id", id);
        //
        JSONObject model = new JSONObject();
        model.put("type", dataType);
        if (dataType.equals("relation")) {
            model.put("related_type_id", relatedTypeId);
        }
        o.put("model", model);
        //
        JSONObject view = new JSONObject();
        view.put("editor", editor);
        o.put("view", view);
        //
        o.put("indexing_mode", indexingMode);
        return o;
    }

    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap();
        properties.put("id", id);
        properties.put("data_type", dataType);
        if (dataType.equals("relation")) {
            properties.put("related_type_id", relatedTypeId);
        }
        properties.put("editor", editor);
        properties.put("indexing_mode", indexingMode);
        return properties;
    }

    // ---

    public DataField setId(String id) {
        this.id = id;
        return this;
    }

    // "text" (default) / "number" / "date" / "html" / "relation"
    public DataField setDataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    // used for dataType="relation" fields
    public DataField setRelatedTypeId(String relatedTypeId) {
        this.relatedTypeId = relatedTypeId;
        return this;
    }

    // "single line" (default) / "multi line"
    public DataField setEditor(String editor) {
        this.editor = editor;
        return this;
    }

    // "OFF" (default) / "KEY" / "FULLTEXT" / "FULLTEXT_KEY"
    public DataField setIndexingMode(String indexingMode) {
        this.indexingMode = indexingMode;
        return this;
    }
}
