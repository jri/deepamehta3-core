package de.deepamehta.core.storage;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;

import java.util.Map;
import java.util.List;
import java.util.Set;



public interface Storage {

    // --- Topics ---

    public Topic getTopic(long id);

    public Topic getTopic(String key, Object value);

    public String getTopicProperty(long topicId, String key);

    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes);

    public List<Topic> searchTopics(String searchTerm, String fieldId, boolean wholeWord);

    public Topic createTopic(String typeId, Map properties);

    public void setTopicProperties(long id, Map properties);

    public void deleteTopic(long id);

    // --- Relations ---

    public Relation getRelation(long srcTopicId, long dstTopicId);

    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties);

    public void deleteRelation(long id);

    // --- Types ---

    public Set<String> getTopicTypeIds();

    public TopicType getTopicType(String typeId);

    public void createTopicType(Map<String, Object> properties, List<DataField> dataFields);

    public void addDataField(String typeId, DataField dataField);

    // --- DB ---

    public Transaction beginTx();

    public void setup();

    public void shutdown();

    public int getDbModelVersion();

    public void setDbModelVersion(int dbModelVersion);
}
