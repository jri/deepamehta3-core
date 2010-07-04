package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



/**
 * The model of a topic, DeepaMehta's core data object.
 * A topic has an ID, a type, a label, and a set of properties.
 * <br><br>
 * Instances of this class are used to pass data around (<i>data transfer object</i>).
 * <br><br>
 * Note: instances of this class are not backed by a database.
 * That is, direct changes to a Topic object (e.g. by {@link #setProperty}) are not persistent.
 * To make persistent changes use the methods of the DeepaMehta core service
 * ({@link de.deepamehta.core.service.DeepaMehtaService}).
 *
 * @author <a href="mailto:jri@deepamehta.de">Jörg Richter</a>
 */
public class Topic {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    public long id;
    public String typeUri;
    public String label;

    protected Map<String, Object> properties;

    // ---------------------------------------------------------------------------------------------------- Constructors

    public Topic(long id, String typeUri, String label, Map properties) {
        this.id = id;
        this.typeUri = typeUri;
        this.label = label;
        this.properties = properties != null ? properties : new HashMap();
    }

    public Topic(Topic topic) {
        this(topic.id, topic.typeUri, topic.label, topic.properties);
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Object getProperty(String key) {
        Object value = properties.get(key);
        if (value == null) {
            throw new RuntimeException("Property \"" + key + "\" of " + this + " is not initialized. " +
                "Remember: topics obtained by getRelatedTopics() provide no properties. " +
                "Use the providePropertiesHook() to initialize the properties you need.");
        }
        return value;
    }

    public Object getProperty(String key, Object defaultValue) {
        Object value = properties.get(key);
        return value != null ? value : defaultValue;
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    /* FIXME: to be dropped
    public void update(Map<String, Object> properties) {
        this.properties.putAll(properties);
    } */

    // ---

    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("id", id);
        o.put("type_uri", typeUri);
        o.put("label", label);
        o.put("properties", properties);
        return o;
    }

    @Override
    public String toString() {
        return "topic " + id + " (typeUri=\"" + typeUri + "\", label=\"" + label + "\", properties=" + properties + ")";
    }
}
