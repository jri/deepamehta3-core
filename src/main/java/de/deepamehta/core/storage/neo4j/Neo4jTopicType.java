package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.TopicType;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
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
        // put in sequence
        Relationship rel;
        if (dataFields.size() == 0) {
            rel = typeNode.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE_START);
        } else {
            Neo4jDataField lastField = (Neo4jDataField) dataFields.get(dataFields.size() - 1);
            rel = lastField.node.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE);
        }
        rel.setProperty("topic_type_id", typeId);
        //
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
            logger.info("  # Result " + fieldNode);
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

    private Iterable<Node> getNodeSequence(Node startNode) {
        String typeId = (String) properties.get("type_id");
        //
        TraversalDescription desc = TraversalFactory.createTraversalDescription();
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE_START, Direction.OUTGOING);
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE,       Direction.OUTGOING);
		// A custom filter is used to return only the nodes of this topic type's individual path.
		// The path is recognized by the "topic_type_id" property of the constitutive relationships.
        desc = desc.filter(new SequenceReturnFilter(typeId));
        // We need breadth first in order to get the nodes in proper sequence order.
        // (default is not breadth first, but probably depth first).
        desc = desc.breadthFirst();
        // We need to traverse a node more than once because it may be involved in many sequences.
        // (default uniqueness is not RELATIONSHIP_GLOBAL, but probably NODE_GLOBAL).
        desc = desc.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL);
        //
        return desc.traverse(startNode).nodes();
    }

    private Node getTypeNode(String typeId) {
        return storage.getMetaClass(typeId).node();
    }

    private class SequenceReturnFilter implements ReturnFilter {

        private String typeId;

        private SequenceReturnFilter(String typeId) {
            logger.info("########## Traversing data field sequence for topic type \"" + typeId + "\"");
            this.typeId = typeId;
        }

        public boolean shouldReturn(Position position) {
            boolean doReturn = !position.atStartNode() &&
                                position.lastRelationship().getProperty("topic_type_id").equals(typeId);
            logger.info("### " + position.node() + " " + position.lastRelationship() + " => return=" + doReturn);
            return doReturn;
        }
    }
}
