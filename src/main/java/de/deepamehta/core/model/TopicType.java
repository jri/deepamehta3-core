package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class TopicType {

    public Map<String, String> properties;
    public List<DataField> dataFields;

    public TopicType(Map properties, List dataFields) {
        this.properties = properties;
        this.dataFields = dataFields;
    }

    public TopicType(JSONObject type) throws JSONException {
        // initialize properties
        properties = new HashMap();
        properties.put("type_id", type.getString("type_id"));
        if (type.has("view")) {
            JSONObject view = type.getJSONObject("view");
            properties.put("icon_src", view.getString("icon_src"));
            if (view.has("label_field")) {
                properties.put("label_field", view.getString("label_field"));
            }
        }
        properties.put("implementation", type.getString("implementation"));
        // initialize data fields
        dataFields = new ArrayList();
        JSONArray fieldDefs = type.getJSONArray("fields");
        for (int i = 0; i < fieldDefs.length(); i++) {
            addDataField(new DataField(fieldDefs.getJSONObject(i)));
        }
    }

    // ---

    public String getProperty(String key) {
        return properties.get(key);
    }

    public DataField getDataField(int index) {
        return dataFields.get(index);
    }

    public DataField getDataField(String id) {
        for (DataField dataField : dataFields) {
            if (dataField.id.equals(id)) {
                return dataField;
            }
        }
        throw new RuntimeException("Topic type \"" + getProperty("type_id") + "\" has no data field \"" + id + "\"");
    }

    // ---

    public void addDataField(DataField dataField) {
        dataFields.add(dataField);
    }
}
