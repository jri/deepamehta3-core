package de.deepamehta.core.model;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONException;

import java.util.HashMap;
import java.util.Map;



/**
 * A relation between 2 {@link Topic}s.
 * A relation has an ID, a type, and a set of properties.
 * <br><br>
 * Instances of this class are used to pass data around (<i>data transfer object</i>).
 * <br><br>
 * Note: instances of this class are not backed by a database.
 * That is, direct changes to a Relation object (e.g. by {@link #setProperty}) are not persistent.
 * To make persistent changes use the methods of the DeepaMehta core service
 * ({@link de.deepamehta.core.service.CoreService}).
 *
 * @author <a href="mailto:jri@deepamehta.de">Jörg Richter</a>
 */
public class Relation {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    public long id;
    public String typeId;
    public long srcTopicId;
    public long dstTopicId;
    public Map<String, Object> properties;

    // ---------------------------------------------------------------------------------------------------- Constructors

    public Relation(long id, String typeId, long srcTopicId, long dstTopicId, Map properties) {
        this.id = id;
        this.typeId = typeId;
        this.srcTopicId = srcTopicId;
        this.dstTopicId = dstTopicId;
        this.properties = properties != null ? properties : new HashMap();
    }

    public Relation(Relation relation) {
        this(relation.id, relation.typeId, relation.srcTopicId, relation.dstTopicId, relation.properties);
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    // ---

    public JSONObject toJSON() throws JSONException {
        JSONObject o = new JSONObject();
        o.put("id", this.id);
        o.put("type_id", this.typeId);
        o.put("src_topic_id", this.srcTopicId);
        o.put("dst_topic_id", this.dstTopicId);
        o.put("properties", this.properties);
        return o;
    }
}
