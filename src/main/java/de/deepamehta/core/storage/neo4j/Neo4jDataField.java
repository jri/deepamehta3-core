package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;

import org.neo4j.graphdb.Node;
import org.neo4j.meta.model.MetaModelProperty;

import java.util.Map;
import java.util.logging.Logger;



/**
 * Backs {@link DataField} by Neo4j database.
 *
 * @author <a href="mailto:jri@deepamehta.de">Jörg Richter</a>
 */
class Neo4jDataField extends DataField {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    MetaModelProperty metaProperty;
    Node node;

    private Logger logger = Logger.getLogger(getClass().getName());

    // ---------------------------------------------------------------------------------------------------- Constructors

    /**
     * Constructs a data field from components read from the database.
     */
    Neo4jDataField(Map properties, Node node) {
        super(properties);
        this.node = node;
    }

    /**
     * Constructs a data field and writes it to the database.
     */
    Neo4jDataField(DataField dataField, Neo4jStorage storage) {
        this.metaProperty = storage.createMetaProperty(dataField.getUri());
        this.node = metaProperty.node();
        logger.info("Creating data field " + dataField + " => ID=" + node.getId());
        setProperties(dataField.getProperties());
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    @Override
    public void setProperty(String key, Object value) {
        logger.info("#################################### Neo4jDataField: set " + key + "=" + value);
        // update memory
        super.setProperty(key, value);
        // update DB
        node.setProperty(key, value);
    }

    @Override
    public void setProperties(Map<String, Object> properties) {
        // update memory
        super.setProperties(properties);
        // update DB
        StringBuilder log = new StringBuilder();
        for (String key : properties.keySet()) {
            Object newValue = properties.get(key);
            Object oldValue = node.getProperty(key, null);
            if (oldValue != null && !oldValue.equals(newValue)) {
                log.append("\n  " + key + ": \"" + oldValue + "\" => \"" + newValue + "\"");
            }
            //
            node.setProperty(key, newValue);
        }
        if (log.length() > 0) {
            logger.warning("Overriding properties of data field " + this + ":" + log);
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
