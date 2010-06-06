package de.deepamehta.core.service;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.plugin.DeepaMehtaPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;



public interface DeepaMehtaService {

    public static final int REQUIRED_DB_MODEL_VERSION = 1;

    // --- Topics ---

    public Topic getTopic(long id);

    public Topic getTopic(String key, Object value);

    public String getTopicProperty(long topicId, String key);

    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes);

    /**
     * Performs a fulltext search.
     *
     * @param   fieldId     The ID of the data field to search. If null is provided all fields are searched.
     * @param   wholeWord   If true the searchTerm is regarded as whole word.
     *                      If false the searchTerm is regarded as begin-of-word substring.
     */
    public Topic searchTopics(String searchTerm, String fieldId, boolean wholeWord);

    public Topic createTopic(String typeId, Map properties);

    public void setTopicProperties(long id, Map properties);

    public void deleteTopic(long id);

    // --- Relations ---

    /**
     * Returns the relation between the two topics (regardless of type and direction).
     * If no such relation exists null is returned.
     * If more than one relation exists, only the first one is returned.
     */
    public Relation getRelation(long srcTopicId, long dstTopicId);

    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties);

    public void deleteRelation(long id);

    // --- Types ---

    public Set<String> getTopicTypeIds();

    public TopicType getTopicType(String typeId);

    public void createTopicType(Map properties, List dataFields);

    public void addDataField(String typeId, DataField dataField);

    // --- Plugins ---

    public void registerPlugin(String pluginId, DeepaMehtaPlugin plugin);

    public void unregisterPlugin(String pluginId);

    public Set<String> getPluginIds();

    public DeepaMehtaPlugin getPlugin(String pluginId);

    public void runPluginMigration(DeepaMehtaPlugin plugin, int migrationNr);

    // --- Misc ---

    public void shutdown();
}
