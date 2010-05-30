package de.deepamehta.core.storage;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.Relation;

import java.util.Map;
import java.util.List;



public interface Storage {

    // --- Topics ---

    public Topic getTopic(long id);

    public Topic getTopic(String key, Object value);

    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes);

    public List<Topic> searchTopics(String searchTerm);

    public Topic createTopic(String typeId, Map properties);

    public void setTopicProperties(long id, Map properties);

    public void deleteTopic(long id);

    // --- Relations ---

    public Relation getRelation(long srcTopicId, long dstTopicId);

    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties);

    public void deleteRelation(long id);

    // --- Types ---

    public void createTopicType(Map<String, Object> properties, List<DataField> dataFields);

    public void addDataField(String typeId, DataField dataField);

    public boolean topicTypeExists(String typeId);

    // --- Misc ---

    public Transaction beginTx();

    public void setup();

    public void shutdown();
}
