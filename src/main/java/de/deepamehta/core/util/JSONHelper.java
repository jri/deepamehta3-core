package de.deepamehta.core.util;

import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Topic;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class JSONHelper {

    // --- Generic ---

    public static Map toMap(JSONObject o) throws JSONException {
        return toMap(o, new HashMap());
    }

    public static Map toMap(JSONObject o, Map map) throws JSONException {
        Iterator<String> i = o.keys();
        while (i.hasNext()) {
            String key = i.next();
            map.put(key, o.get(key));
        }
        return map;
    }

    public static List toList(JSONArray o) throws JSONException {
        List list = new ArrayList();
        for (int i = 0; i < o.length(); i++) {
            list.add(o.get(i));
        }
        return list;
    }

    // --- DeepaMehta specific ---

    public static JSONArray topicsToJson(List<Topic> topics) throws JSONException {
        JSONArray array = new JSONArray();
        for (Topic topic : topics) {
            array.put(topic.toJSON());
        }
        return array;
    }

    // FIXME: for the moment it is sufficient to serialize the topics only. The respective relations are omitted.
    public static JSONArray relatedTopicsToJson(List<RelatedTopic> relTopics) throws JSONException {
        JSONArray array = new JSONArray();
        for (RelatedTopic relTopic : relTopics) {
            array.put(relTopic.getTopic().toJSON());
        }
        return array;
    }

    // ---

    /**
      * Converts a "Cookie" header value (String) to a map (key=String, value=String).
      * E.g. "user=jri; workspace_id=123" => {"user"="jri", "workspace_id"="123"}
      */
    public static Map<String, String> cookieToMap(String cookie) {
        Map cookieValues = new HashMap();
        if (cookie != null) {
            for (String value : cookie.split("; ")) {
                String[] val = value.split("=");
                cookieValues.put(val[0], val[1]);
            }
        }
        return cookieValues;
    }
}
