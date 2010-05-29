package de.deepamehta.core.impl;

import de.deepamehta.core.model.TopicType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;



public class TypeCache {

    private Map<String, TopicType> topicTypes = new HashMap();

    void addTopicType(TopicType topicType) {
        String typeId = topicType.getProperty("type_id");
        topicTypes.put(typeId, topicType);
    }

    public TopicType getTopicType(String typeId) {
        TopicType topicType = topicTypes.get(typeId);
        if (topicType == null) {
            throw new RuntimeException("Topic type \"" + typeId + "\" not found in type cache");
        }
        return topicType;
    }

    Collection<TopicType> getTopicTypes() {
        return topicTypes.values();
    }
}
