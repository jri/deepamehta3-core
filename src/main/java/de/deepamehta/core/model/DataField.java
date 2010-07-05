package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



/**
 * A data field. Part of the meta-model (like an attribute). A data field is part of a {@link TopicType}.
 * <br><br>
 * A data field has a label and a data type.
 * A data field is identified by an URI.
 * Furthermore a data field has a) an indexing mode which controls the indexing of the data field's value, and
 * b) hints which control the building of an corresponding editor widget.
 *
 * @author <a href="mailto:jri@deepamehta.de">Jörg Richter</a>
 */
public class DataField {
    
    // ---------------------------------------------------------------------------------------------- Instance Variables

    public String uri;
    public String dataType;
    public String relatedTypeUri;    // used for dataType="relation" fields
    public String label;
    public String editor;
    public String indexingMode;

    // ---------------------------------------------------------------------------------------------------- Constructors

    public DataField() {
        setDataType("text");
        setEditor("single line");
        setIndexingMode("OFF");
    }

    public DataField(String label) {
        this();
        setLabel(label);
    }

    public DataField(Map properties) {
        update(properties);
    }

    public DataField(JSONObject dataField) {
        this();
        try {
            setUri(dataField.getString("uri"));
            // parse "model"
            if (dataField.has("model")) {
                JSONObject model = dataField.getJSONObject("model");
                if (model.has("type")) {
                    setDataType(model.getString("type"));
                }
                if (dataType.equals("relation")) {
                    setRelatedTypeUri(model.getString("related_type_uri"));
                }
            }
            // parse "view"
            JSONObject view = dataField.getJSONObject("view");
            setLabel(view.getString("label"));
            if (view.has("editor")) {
                setEditor(view.getString("editor"));
            }
            // parse "indexing_mode"
            if (dataField.has("indexing_mode")) {
                setIndexingMode(dataField.getString("indexing_mode"));
            }
        } catch (Throwable e) {
            throw new RuntimeException("Error while parsing data field \"" + uri + "\"", e);
        }
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("uri", uri);
        //
        JSONObject model = new JSONObject();
        model.put("type", dataType);
        if (dataType.equals("relation")) {
            model.put("related_type_uri", relatedTypeUri);
        }
        o.put("model", model);
        //
        JSONObject view = new JSONObject();
        view.put("label", label);
        view.put("editor", editor);
        o.put("view", view);
        //
        o.put("indexing_mode", indexingMode);
        return o;
    }

    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap();
        properties.put("uri", uri);
        properties.put("data_type", dataType);
        if (dataType.equals("relation")) {
            properties.put("related_type_uri", relatedTypeUri);
        }
        properties.put("label", label);
        properties.put("editor", editor);
        properties.put("indexing_mode", indexingMode);
        return properties;
    }

    public void update(Map<String, String> properties) {
        setUri(properties.get("uri"));
        setDataType(properties.get("data_type"));
        setRelatedTypeUri(properties.get("related_type_uri"));
        setLabel(properties.get("label"));
        setEditor(properties.get("editor"));
        setIndexingMode(properties.get("indexing_mode"));
    }

    // ---

    public DataField setUri(String uri) {
        this.uri = uri;
        return this;
    }

    // "text" (default) / "number" / "date" / "html" / "relation"
    public DataField setDataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    // used for dataType="relation" fields
    public DataField setRelatedTypeUri(String relatedTypeUri) {
        this.relatedTypeUri = relatedTypeUri;
        return this;
    }

    public DataField setLabel(String label) {
        this.label = label;
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

    // ---

    @Override
    public boolean equals(Object o) {
        return ((DataField) o).uri.equals(uri);
    }

    @Override
    public String toString() {
        return "data field \"" + label + "\" (uri=\"" + uri + "\" dataType=\"" + dataType + "\" relatedTypeUri=\"" +
            relatedTypeUri + "\" editor=\"" + editor + "\" indexingMode=\"" + indexingMode + "\")";
    }
}
