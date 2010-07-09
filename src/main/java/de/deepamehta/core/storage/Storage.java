package de.deepamehta.core.storage;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;

import java.util.Map;
import java.util.List;
import java.util.Set;



/**
 * Specification of the DeepaMehta storage layer in a DBMS-agnostic way.
 */
public interface Storage {

    // --- Topics ---

    public Topic getTopic(long id);

    /**
     * Looks up a topic by exact property value.
     * If no such topic exists <code>null</code> is returned.
     * If more than one topic is found a runtime exception is thrown.
     * <br><br>
     * IMPORTANT: Looking up a topic this way requires the property to be indexed with indexing mode <code>KEY</code>.
     * This is achieved by declaring the respective data field with <code>indexing_mode: "KEY"</code>
     * (for statically declared data field, typically in <code>types.json</code>) or
     * by calling DataField's {@link DataField#setIndexingMode} method with <code>"KEY"</code> as argument
     * (for dynamically created data fields, typically in migration classes).
     */
    public Topic getTopic(String key, Object value);

    /**
     * Returns a property value of a topic, or <code>null</code> if the topic doesn't have such a property.
     */
    public Object getTopicProperty(long topicId, String key);

    public List<Topic> getTopics(String typeUri);

    public List<RelatedTopic> getRelatedTopics(long topicId, List<String> includeTopicTypes,
                                                             List<String> includeRelTypes,
                                                             List<String> excludeRelTypes);

    public List<Topic> searchTopics(String searchTerm, String fieldUri, boolean wholeWord);

    public Topic createTopic(String typeUri, Map properties);

    public void setTopicProperties(long id, Map properties);

    /**
     * Deletes the topic and all of its relations.
     */
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

    public TopicType createTopicType(Map<String, Object> properties, List<DataField> dataFields);

    public void addDataField(String typeUri, DataField dataField);

    public void updateDataField(String typeUri, DataField dataField);

    public void removeDataField(String typeUri, String fieldUri);

    public void setDataFieldOrder(String typeUri, List fieldUris);

    // --- DB ---

    public Transaction beginTx();

    public void init();

    public void shutdown();

    public int getDbModelVersion();

    public void setDbModelVersion(int dbModelVersion);
}
