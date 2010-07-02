package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.TopicType;

import org.neo4j.commons.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
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
 * This class extends TopicType to provide persistence by the means of Neo4j.
 */
class Neo4jTopicType extends TopicType {

    // ---------------------------------------------------------------------------------------------- Instance Variables

    Node typeNode;
    Neo4jStorage storage;

    private Logger logger = Logger.getLogger(getClass().getName());

    // ---------------------------------------------------------------------------------------------------- Constructors

    /**
     * Constructs a topic type and writes it to the database.
     */
    Neo4jTopicType(Map<String, Object> properties, List<DataField> dataFields, Neo4jStorage storage) {
        super(properties, new ArrayList());
        this.storage = storage;
        // create type
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        MetaModelClass metaClass = storage.createMetaClass(typeUri);
        this.typeNode = metaClass.node();
        this.id = typeNode.getId();
        logger.info("Creating topic type \"" + typeUri + "\" => ID=" + id);
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
    Neo4jTopicType(String typeUri, Neo4jStorage storage) {
        super(null, null);
        this.storage = storage;
        this.typeNode = getTypeNode(typeUri);
        this.properties = storage.getProperties(typeNode);
        this.dataFields = readDataFields();
        this.id = typeNode.getId();
    }

    // -------------------------------------------------------------------------------------------------- Public Methods

    @Override
    public Neo4jDataField getDataField(int index) {
        return (Neo4jDataField) super.getDataField(index);
    }

    @Override
    public Neo4jDataField getDataField(String uri) {
        return (Neo4jDataField) super.getDataField(uri);
    }

    // ---

    /**
     * Adds a data field to this topic type and writes the data field to the database.
     */
    @Override
    public void addDataField(DataField dataField) {
        // 1) update DB
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        // create data field
        Neo4jDataField field = new Neo4jDataField(dataField, storage);
        storage.getMetaClass(typeUri).getDirectProperties().add(field.getMetaProperty());
        // put in sequence
        Node fieldNode = field.node;
        if (dataFields.size() == 0) {
            startFieldSequence(fieldNode);
        } else {
            addToFieldSequence(fieldNode, dataFields.size() - 1);
        }
        // 2) update memory
        super.addDataField(field);
    }

    @Override
    public void removeDataField(String uri) {
        Neo4jDataField field = getDataField(uri);    // the data field to remove
        int index = dataFields.indexOf(field);      // the index of the data field to remove
        if (index == -1) {
            throw new RuntimeException("List.indexOf() returned -1");
        }
        // 1) update DB
        // repair sequence
        if (index == 0) {
            if (dataFields.size() > 1) {
                // The data field to remove is the first one and further data fields exist.
                // -> Make the next data field the new start data field.
                Node nextFieldNode = getDataField(index + 1).node;
                startFieldSequence(nextFieldNode);
            }
        } else {
            if (index < dataFields.size() - 1) {
                // The data field to remove is surrounded by other data fields.
                // -> Make the surrounding data fields direct neighbours.
                Node nextFieldNode = getDataField(index + 1).node;
                addToFieldSequence(nextFieldNode, index - 1);
            }
        }
        // delete the data field topic including all of its (sequence) relations
        storage.deleteTopic(field.node.getId());
        // 2) update memory
        super.removeDataField(uri);
    }

    // ------------------------------------------------------------------------------------------------- Private Methods

    private void startFieldSequence(Node fieldNode) {
        Relationship rel = typeNode.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE_START);
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        rel.setProperty("type_uri", typeUri);
    }

    private void addToFieldSequence(Node fieldNode, int prevIndex) {
        Node prevFieldNode = getDataField(prevIndex).node;
        Relationship rel = prevFieldNode.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE);
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        rel.setProperty("type_uri", typeUri);
    }

    // ---

    /**
     * Reads the data fields of this topic type from the database.
     */
    private List<DataField> readDataFields() {
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        // use as control group
        List propNodes = new ArrayList();
        for (MetaModelProperty metaProp : storage.getMetaClass(typeUri).getDirectProperties()) {
            propNodes.add(metaProp.node());
        }
        //
        List dataFields = new ArrayList();
        for (Node fieldNode : getNodeSequence(typeNode)) {
            logger.fine("  # Result " + fieldNode);
            // error check
            if (!propNodes.contains(fieldNode)) {
                throw new RuntimeException("Graph inconsistency for topic type \"" + typeUri + "\": " +
                    fieldNode + " appears in data field sequence but is not a meta property node");
            }
            //
            dataFields.add(new Neo4jDataField(storage.getProperties(fieldNode), fieldNode));
        }
        // error check
        if (propNodes.size() != dataFields.size()) {
            throw new RuntimeException("Graph inconsistency for topic type \"" + typeUri + "\": there are " +
                dataFields.size() + " nodes in data field sequence but " + propNodes.size() + " meta property nodes");
        }
        //
        return dataFields;
    }

    private Iterable<Node> getNodeSequence(Node startNode) {
        String typeUri = (String) properties.get("http://www.deepamehta.de/core/property/TypeURI");
        //
        TraversalDescription desc = TraversalFactory.createTraversalDescription();
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE_START, Direction.OUTGOING);
        desc = desc.relationships(Neo4jStorage.RelType.SEQUENCE,       Direction.OUTGOING);
        // A custom filter is used to return only the nodes of this topic type's individual path.
        // The path is recognized by the "type_uri" property of the constitutive relationships.
        desc = desc.filter(new SequenceReturnFilter(typeUri));
        // We need breadth first in order to get the nodes in proper sequence order.
        // (default is not breadth first, but probably depth first).
        desc = desc.breadthFirst();
        // We need to traverse a node more than once because it may be involved in many sequences.
        // (default uniqueness is not RELATIONSHIP_GLOBAL, but probably NODE_GLOBAL).
        desc = desc.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL);
        //
        return desc.traverse(startNode).nodes();
    }

    private Node getTypeNode(String typeUri) {
        return storage.getMetaClass(typeUri).node();
    }

    private class SequenceReturnFilter implements Predicate {

        private String typeUri;

        private SequenceReturnFilter(String typeUri) {
            logger.fine("########## Traversing data field sequence for topic type \"" + typeUri + "\"");
            this.typeUri = typeUri;
        }

        @Override
        public boolean accept(Object item) {
            Position position = (Position) item;
            boolean doReturn = !position.atStartNode() &&
                                position.lastRelationship().getProperty("type_uri").equals(typeUri);
            logger.fine("### " + position.node() + " " + position.lastRelationship() + " => return=" + doReturn);
            return doReturn;
        }
    }
}
