package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;



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
