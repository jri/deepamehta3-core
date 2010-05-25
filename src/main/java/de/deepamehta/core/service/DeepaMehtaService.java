package de.deepamehta.core.service;

import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.plugin.DeepaMehtaPlugin;

import java.util.List;
import java.util.Map;
import java.util.Set;



public interface DeepaMehtaService {

    // --- Topics ---

    public Topic getTopic(long id);

    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes);

    public Topic searchTopics(String searchTerm);

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

    public void createTopicType(Map properties, List dataFields);

    public boolean topicTypeExists(String typeId);

    // --- Plugins ---

    public void registerPlugin(String pluginId, DeepaMehtaPlugin plugin);

    public void unregisterPlugin(String pluginId);

    public Set<String> getPluginIds();

    public DeepaMehtaPlugin getPlugin(String pluginId);

    // --- Misc ---

    public void shutdown();
}
