package de.deepamehta.core.util;

import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.service.CoreService;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;



public class JSONHelper {

    private static Logger logger = Logger.getLogger("de.deepamehta.core.util.JSONHelper");

    // === Generic ===

    public static Map toMap(JSONObject o) {
        return toMap(o, new HashMap());
    }

    public static Map toMap(JSONObject o, Map map) {
        try {
            Iterator<String> i = o.keys();
            while (i.hasNext()) {
                String key = i.next();
                map.put(key, o.get(key));   // throws JSONException
            }
            return map;
        } catch (JSONException e) {
            throw new RuntimeException("Error while converting JSONObject to Map", e);
        }
    }

    public static List toList(JSONArray o) {
        try {
            List list = new ArrayList();
            for (int i = 0; i < o.length(); i++) {
                list.add(o.get(i));         // throws JSONException
            }
            return list;
        } catch (JSONException e) {
            throw new RuntimeException("Error while converting JSONArray to Map", e);
        }
    }

    // === DeepaMehta specific ===

    public static JSONArray topicsToJson(List<Topic> topics) {
        JSONArray array = new JSONArray();
        for (Topic topic : topics) {
            array.put(topic.toJSON());
        }
        return array;
    }

    public static JSONArray relationsToJson(List<Relation> relations) {
        JSONArray array = new JSONArray();
        for (Relation relation : relations) {
            array.put(relation.toJSON());
        }
        return array;
    }

    // FIXME: for the moment it is sufficient to serialize the topics only. The respective relations are omitted.
    public static JSONArray relatedTopicsToJson(List<RelatedTopic> relTopics) {
        JSONArray array = new JSONArray();
        for (RelatedTopic relTopic : relTopics) {
            array.put(relTopic.getTopic().toJSON());
        }
        return array;
    }

    // ---

    /**
     * Creates types from a JSON formatted input stream.
     */
    public static void readTypesFromFile(InputStream is, String typesFileName, CoreService cs) {
        try {
            InputStreamReader reader = new InputStreamReader(is);
            logger.info("Reading types from file \"" + typesFileName +
                "\" (assumed encoding is \"" + reader.getEncoding() + "\")");
            BufferedReader in = new BufferedReader(reader);
            String line;
            StringBuilder json = new StringBuilder();
            while ((line = in.readLine()) != null) {
                json.append(line);
            }
            createTypes(json.toString(), cs);
        } catch (Throwable e) {
            throw new RuntimeException("Error while reading types file \"" + typesFileName + "\"", e);
        }
    }

    public static void createTypes(String json, CoreService cs) throws JSONException {
        JSONArray types = new JSONArray(json);
        for (int i = 0; i < types.length(); i++) {
            TopicType topicType = new TopicType(types.getJSONObject(i));
            cs.createTopicType(topicType.getProperties(), topicType.getDataFields());
        }
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
