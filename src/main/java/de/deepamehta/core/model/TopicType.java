package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * The model of a topic type. A topic type is basically an ordered collection of data fields.
 * A topic type is itself a topic.
 *
 * @see DataField
 * @see Topic
 *
 * @author <a href="mailto:jri@deepamehta.de">Jörg Richter</a>
 */
public class TopicType extends Topic {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    protected List<DataField> dataFields;

    // ---------------------------------------------------------------------------------------------------- Constructors

    public TopicType(Map properties, List dataFields) {
        // id and label remain uninitialized
        super(-1, "http://www.deepamehta.de/core/topictype/TopicType", null, properties);
        this.dataFields = dataFields;
    }

    public TopicType(JSONObject type) {
        // id, label, and properties remain uninitialized
        super(-1, "http://www.deepamehta.de/core/topictype/TopicType", null, null);
        try {
            // initialize properties
            setProperty("http://www.deepamehta.de/core/property/TypeURI", type.getString("uri"));
            JSONObject view = type.getJSONObject("view");
            setProperty("http://www.deepamehta.de/core/property/TypeName", view.getString("label"));
            if (view.has("icon_src")) {
                setProperty("icon_src", view.getString("icon_src"));
            }
            if (view.has("label_field")) {
                setProperty("label_field", view.getString("label_field"));
            }
            setProperty("implementation", type.getString("implementation"));
            // initialize data fields
            dataFields = new ArrayList();
            JSONArray fieldDefs = type.getJSONArray("fields");
            for (int i = 0; i < fieldDefs.length(); i++) {
                addDataField(new DataField(fieldDefs.getJSONObject(i)));
            }
        } catch (Throwable e) {
            throw new RuntimeException("Error while parsing topic type \"" +
                getProperty("http://www.deepamehta.de/core/property/TypeURI") + "\"", e);
        }
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("uri", getProperty("http://www.deepamehta.de/core/property/TypeURI"));
        //
        JSONArray fields = new JSONArray();
        for (DataField dataField : dataFields) {
            fields.put(dataField.toJSON());
        }
        o.put("fields", fields);
        //
        JSONObject view = new JSONObject();
        view.put("label", getProperty("http://www.deepamehta.de/core/property/TypeName", null));
        view.put("icon_src", getProperty("icon_src", null));
        view.put("label_field", getProperty("label_field", null));
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

    public DataField getDataField(String uri) {
        for (DataField dataField : dataFields) {
            if (dataField.uri.equals(uri)) {
                return dataField;
            }
        }
        throw new RuntimeException("Topic type \"" + getProperty("http://www.deepamehta.de/core/property/TypeName") +
            "\" has no data field \"" + uri + "\"");
    }

    // ---

    public void addDataField(DataField dataField) {
        dataFields.add(dataField);
    }

    public void removeDataField(String uri) {
        try {
            boolean removed = dataFields.remove(getDataField(uri));
            if (!removed) {
                throw new RuntimeException("List.remove() returned false");
            }
        } catch (Throwable e) {
            throw new RuntimeException("Data field \"" + uri + "\" can't be removed", e);
        }
    }
}
