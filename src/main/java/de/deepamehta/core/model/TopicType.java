package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class TopicType extends Topic {

    public List<DataField> dataFields;

    public TopicType(Map properties, List dataFields) {
        super(-1, "Topic Type", null, properties);      // id and label remain uninitialized
        this.dataFields = dataFields;
    }

    public TopicType(JSONObject type) throws JSONException {
        super(-1, "Topic Type", null, new HashMap());   // id and label remain uninitialized
        // initialize properties
        setProperty("type_id", type.getString("type_id"));
        if (type.has("view")) {
            JSONObject view = type.getJSONObject("view");
            setProperty("icon_src", view.getString("icon_src"));
            if (view.has("label_field")) {
                setProperty("label_field", view.getString("label_field"));
            }
        }
        setProperty("implementation", type.getString("implementation"));
        // initialize data fields
        dataFields = new ArrayList();
        JSONArray fieldDefs = type.getJSONArray("fields");
        for (int i = 0; i < fieldDefs.length(); i++) {
            addDataField(new DataField(fieldDefs.getJSONObject(i)));
        }
    }

    // ---

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("type_id", getProperty("type_id"));
        //
        JSONArray fields = new JSONArray();
        for (DataField dataField : dataFields) {
            fields.put(dataField.toJSON());
        }
        o.put("fields", fields);
        //
        JSONObject view = new JSONObject();
        view.put("icon_src", getProperty("icon_src"));
        view.put("label_field", getProperty("label_field"));
        o.put("view", view);
        //
        o.put("implementation", getProperty("implementation"));
        return o;
    }

    // ---

    public List<DataField> getDataFields() {
        return dataFields;
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
