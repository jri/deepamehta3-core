package de.deepamehta.core.impl;

import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.service.DeepaMehtaService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;



public class TypeCache {

    private Map<String, TopicType> topicTypes = new HashMap();
    private DeepaMehtaService dms;

    private Logger logger = Logger.getLogger(getClass().getName());

    TypeCache(DeepaMehtaService dms) {
        this.dms = dms;
    }

    public TopicType getTopicType(String typeId) {
        TopicType topicType = topicTypes.get(typeId);
        if (topicType == null) {
            logger.info("Loading topic type \"" + typeId + "\" into type cache");
            topicType = dms.getTopicType(typeId);
            put(topicType);
        }
        return topicType;
    }

    public void put(TopicType topicType) {
        String typeId = topicType.getProperty("type_id");
        topicTypes.put(typeId, topicType);
    }

    /* public void invalidate(String typeId) {
        if (topicTypes.remove(typeId) != null) {
            logger.info("Removing topic type \"" + typeId + "\" from type cache");
        }
    } */
}
