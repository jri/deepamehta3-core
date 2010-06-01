package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.service.DeepaMehtaService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;



public class TypeCache {

    private Map<String, TopicType> topicTypes = new HashMap();
    private Neo4jStorage storage;

    private Logger logger = Logger.getLogger(getClass().getName());

    TypeCache(Neo4jStorage storage) {
        this.storage = storage;
    }

    // ---

    public TopicType get(String typeId) {
        TopicType topicType = topicTypes.get(typeId);
        if (topicType == null) {
            logger.info("Loading topic type \"" + typeId + "\" into type cache");
            topicType = new Neo4jTopicType(typeId, storage);
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
