package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;

import org.neo4j.graphdb.Node;
import org.neo4j.meta.model.MetaModelProperty;

import java.util.Map;
import java.util.logging.Logger;



class Neo4jDataField extends DataField {

    MetaModelProperty metaProperty;
    Node node;

    private Logger logger = Logger.getLogger(getClass().getName());

    // ---

    Neo4jDataField(Map properties, Node node) {
        super(properties);
        this.node = node;
    }

    /**
     * Writes the data field to the database.
     */
    Neo4jDataField(DataField dataField, Neo4jStorage storage) {
        metaProperty = storage.createMetaProperty(dataField.id);
        node = metaProperty.node();
        logger.info("Creating " + dataField + " => ID=" + node.getId());
        // set properties
        update(dataField.getProperties());
    }

    // ---

    @Override
    public void update(Map<String, String> properties) {
        super.update(properties);
        for (String key : properties.keySet()) {
            node.setProperty(key, properties.get(key));
        }
    }

    // ---

    MetaModelProperty getMetaProperty() {
        return metaProperty;
    }

    Node getNode() {
        return node;
    }
}
