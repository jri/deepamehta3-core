package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.TopicType;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.TraversalFactory;
import org.neo4j.meta.model.MetaModelClass;
import org.neo4j.meta.model.MetaModelProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;



/**
 * Extension of the topic type model that can read/write topic types from/to a Neo4j database.
 */
class Neo4jTopicType extends TopicType {

    Node typeNode;
    Neo4jStorage storage;

    private Logger logger = Logger.getLogger(getClass().getName());

    Neo4jTopicType(Map<String, Object> properties, List<DataField> dataFields, Neo4jStorage storage) {
        super(properties, new ArrayList());
        this.storage = storage;
        // create type
        String typeId = (String) properties.get("type_id");
        MetaModelClass metaClass = storage.createMetaClass(typeId);
        this.typeNode = metaClass.node();
        logger.info("Creating topic type \"" + typeId + "\" => ID=" + typeNode.getId());
        // set properties
        for (String key : properties.keySet()) {
            typeNode.setProperty(key, properties.get(key));
        }
        // add data fields
        for (DataField dataField : dataFields) {
            addDataField(dataField);
        }
    }

    /**
     * Reads a topic type from the database.
     */
    Neo4jTopicType(String typeId, Neo4jStorage storage) {
        super(null, null);
        this.storage = storage;
        this.typeNode = getTypeNode(typeId);
        this.properties = storage.getProperties(typeNode);
        this.dataFields = getDataFields(typeId);
    }

    // ---

    @Override
    public void addDataField(DataField dataField) {
        String typeId = (String) properties.get("type_id");
        // create data field
        MetaModelProperty metaProperty = storage.createMetaProperty(dataField.id);
        Node fieldNode = metaProperty.node();
        logger.info("Creating data field \"" + dataField.id + "\" for topic type \"" +
            typeId + "\" => ID=" + fieldNode.getId());
        storage.getMetaClass(typeId).getDirectProperties().add(metaProperty);
        // set properties
        Map<String, String> properties = dataField.getProperties();
        for (String key : properties.keySet()) {
            fieldNode.setProperty(key, properties.get(key));
        }
        //
        if (dataFields.size() == 0) {
            typeNode.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE_START);
        } else {
            Neo4jDataField lastField = (Neo4jDataField) dataFields.get(dataFields.size() - 1);
            lastField.node.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE);
        }
        super.addDataField(new Neo4jDataField(properties, fieldNode));
    }

    // ---

    private List<DataField> getDataFields(String typeId) {
        // use as control group
        List propNodes = new ArrayList();
        for (MetaModelProperty metaProp : storage.getMetaClass(typeId).getDirectProperties()) {
            propNodes.add(metaProp.node());
        }
        //
        List dataFields = new ArrayList();
        for (Node fieldNode : getNodeSequence(typeNode)) {
            // error check
            if (!propNodes.contains(fieldNode)) {
                throw new RuntimeException("Graph inconsistency for topic type \"" + typeId + "\": " +
                    fieldNode + " appears in data field sequence but is not a meta property node");
            }
            //
            dataFields.add(new Neo4jDataField(storage.getProperties(fieldNode), fieldNode));
        }
        // error check
        if (propNodes.size() != dataFields.size()) {
            throw new RuntimeException("Graph inconsistency for topic type \"" + typeId + "\": there are " +
                dataFields.size() + " nodes in data field sequence but " + propNodes.size() + " meta property nodes");
        }
        //
        return dataFields;
    }

    private Iterable<Node> getNodeSequence(Node referenceNode) {
        TraversalDescription desc = TraversalFactory.createTraversalDescription();
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE_START, Direction.OUTGOING);
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE,       Direction.OUTGOING);
        desc = desc.filter(ReturnFilter.ALL_BUT_START_NODE);
        //
        return desc.traverse(referenceNode).nodes();
    }

    private Node getTypeNode(String typeId) {
        return storage.getMetaClass(typeId).node();
    }
}
