package de.deepamehta.core.service;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Relation;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;



public interface DeepaMehtaService {

    // --- Topics ---

    public Topic getTopic(long id);

    public Topic getTopic(String key, Object value);

    public Object getTopicProperty(long topicId, String key);

    public List<Topic> getTopics(String typeUri);

    /**
     * Retrives topics and relationships that are directly connected to the given topic, optionally filtered
     * by topic types and relation types.
     *
     * IMPORTANT: the topics and relations returned by this method provide no properties.
     * To initialize the properties needed by your plugin define its providePropertiesHook().
     *
     * @param   includeRelTypes     The include relation type filter (optional).
     *                              A list of strings of the form "<relTypeName>[;<direction>]",
     *                              e.g. "TOPICMAP_TOPIC;INCOMING".
     *                              Null or an empty list switches the filter off.
     * @param   excludeRelTypes     The exclude relation type filter (optional).
     *                              A list of strings of the form "<relTypeName>[;<direction>]",
     *                              e.g. "SEARCH_RESULT;OUTGOING".
     *                              Null or an empty list switches the filter off.
     *
     * @return  The related topics, each one as a pair: the topic (a Topic object), and the connecting relation
     *          (a Relation object).
     */
    public List<RelatedTopic> getRelatedTopics(long topicId, List<String> includeTopicTypes,
                                                             List<String> includeRelTypes,
                                                             List<String> excludeRelTypes);

    /**
     * Performs a fulltext search.
     *
     * @param   fieldUri    The URI of the data field to search. If null is provided all fields are searched.
     * @param   wholeWord   If true the searchTerm is regarded as whole word.
     *                      If false the searchTerm is regarded as begin-of-word substring.
     */
    public Topic searchTopics(String searchTerm, String fieldUri, boolean wholeWord, Map clientContext);

    public Topic createTopic(String typeUri, Map properties, Map clientContext);

    public void setTopicProperties(long id, Map properties);

    public void deleteTopic(long id);

    // --- Relations ---

    public Relation getRelation(long id);

    /**
     * Returns the relation between the two topics (regardless of type and direction).
     * If no such relation exists null is returned.
     * If more than one relation exists, only the first one is returned.
     */
    public Relation getRelation(long srcTopicId, long dstTopicId);

    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties);

    public void setRelationProperties(long id, Map properties);

    public void deleteRelation(long id);

    // --- Types ---

    public Set<String> getTopicTypeUris();

    public TopicType getTopicType(String typeUri);

    public TopicType createTopicType(Map properties, List dataFields);

    public void addDataField(String typeUri, DataField dataField);

    public void updateDataField(String typeUri, DataField dataField);

    public void removeDataField(String typeUri, String fieldUri);

    public void setDataFieldOrder(String typeUri, List fieldUris);

    // --- Commands ---

    public JSONObject executeCommand(String command, Map params, Map clientContext);

    // --- Plugins ---

    public void registerPlugin(String pluginId, Plugin plugin);

    public void unregisterPlugin(String pluginId);

    public Set<String> getPluginIds();

    public Plugin getPlugin(String pluginId);

    public void runPluginMigration(Plugin plugin, int migrationNr);

    // --- Misc ---

    public void shutdown();
}
