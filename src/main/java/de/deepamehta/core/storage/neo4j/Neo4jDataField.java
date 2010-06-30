package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;

import org.neo4j.graphdb.Node;
import org.neo4j.meta.model.MetaModelProperty;

import java.util.Map;
import java.util.logging.Logger;



/**
 * This class extends DataField to provide persistence by the means of Neo4j.
 */
class Neo4jDataField extends DataField {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    MetaModelProperty metaProperty;
    Node node;

    private Logger logger = Logger.getLogger(getClass().getName());

    // ---------------------------------------------------------------------------------------------------- Constructors

    Neo4jDataField(Map properties, Node node) {
        super(properties);
        this.node = node;
    }

    /**
     * Constructs a data field and writes it to the database.
     */
    Neo4jDataField(DataField dataField, Neo4jStorage storage) {
        metaProperty = storage.createMetaProperty(dataField.uri);
        node = metaProperty.node();
        logger.info("Creating " + dataField + " => ID=" + node.getId());
        // set properties
        update(dataField.getProperties());
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    @Override
    public void update(Map<String, String> properties) {
        super.update(properties);
        for (String key : properties.keySet()) {
            // FIXME: checks to be dropped
            if (node == null) logger.warning("### node==null (" + this +")");
            if (key  == null) logger.warning("### key==null (" + this +")");
            if (properties == null) logger.warning("### properties==null (" + this +")");
            //
            node.setProperty(key, properties.get(key));
        }
    }

    // ----------------------------------------------------------------------------------------- Package Private Methods

    MetaModelProperty getMetaProperty() {
        return metaProperty;
    }

    Node getNode() {
        return node;
    }
}
