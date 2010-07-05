package de.deepamehta.core.impl.plugins;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.service.Plugin;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;



public class DefaultPlugin extends Plugin {

    private Logger logger = Logger.getLogger(getClass().getName());



    public DefaultPlugin() {
        configProperties = new Properties();
    }



    // ************************
    // *** Overriding Hooks ***
    // ************************



    @Override
    public void postUpdateHook(Topic topic, Map<String, Object> oldProperties) {
        if (topic.typeUri.equals("http://www.deepamehta.de/core/topictype/TopicType")) {
            // update type URI
            String oldTypeUri = (String) oldProperties.get("http://www.deepamehta.de/core/property/TypeURI");
            String newTypeUri = (String) topic.getProperty("http://www.deepamehta.de/core/property/TypeURI");
            if (!oldTypeUri.equals(newTypeUri)) {
                logger.info("### Changing type URI from \"" + oldTypeUri + "\" to \"" + newTypeUri + "\"");
                dms.getTopicType(oldTypeUri).setTypeUri(newTypeUri);
            }
            // update type label
            String oldTypeLabel = (String) oldProperties.get("http://www.deepamehta.de/core/property/TypeName");
            String newTypeLabel = (String) topic.getProperty("http://www.deepamehta.de/core/property/TypeName");
            if (!oldTypeLabel.equals(newTypeLabel)) {
                logger.info("### Changing type label from \"" + oldTypeLabel + "\" to \"" + newTypeLabel + "\"");
                dms.getTopicType(newTypeUri).setLabel(newTypeLabel);
            }
        }
    }
}
