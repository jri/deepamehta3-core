package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.storage.Storage;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.index.lucene.LuceneFulltextQueryIndexService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;



public class Neo4jStorage implements Storage {

    private Logger logger = Logger.getLogger(getClass().getName());

    private final GraphDatabaseService graphDb;
    private final IndexService index;
    private final LuceneFulltextQueryIndexService fulltextIndex;
    //
    Map<String, TopicType> topicTypes;

    private enum RelType implements RelationshipType {
        TOPIC_TYPE, DATA_FIELD, INSTANCE,
        RELATION, SEARCH_RESULT
    }

    public Neo4jStorage(String dbPath, Map topicTypes) {
        logger.info("Creating DB and indexing services");
        graphDb = new EmbeddedGraphDatabase(dbPath);
        index = new LuceneIndexService(graphDb);
        fulltextIndex = new LuceneFulltextQueryIndexService(graphDb);
        this.topicTypes = topicTypes;
    }



    // ******************************
    // *** Storage Implementation ***
    // ******************************



    // --- Topics ---

    @Override
    public Topic getTopic(long id) {
        logger.info("Getting node " + id);
        Node node = graphDb.getNodeById(id);
        return new Topic(id, getTypeId(node), null, getProperties(node));     // FIXME: label remains uninitialized
    }

    @Override
    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes) {
        logger.info("Getting related nodes of node " + topicId);
        List topics = new ArrayList();
        Node node = graphDb.getNodeById(topicId);
        for (Relationship rel : node.getRelationships()) {
            if (!excludeRelTypes.contains(rel.getType().name())) {
                Node relNode = rel.getOtherNode(node);
                topics.add(buildTopic(relNode));
            }
        }
        return topics;
    }

    @Override
    public List<Topic> searchTopics(String searchTerm) {
        IndexHits<Node> hits = fulltextIndex.getNodes("default", searchTerm + "*"); // FIXME: do more itelligent manipulation on the search term
        logger.info("Searching \"" + searchTerm + "\" => " + hits.size() + " nodes found");
        List result = new ArrayList();
        for (Node node : hits) {
            result.add(new Topic(node.getId(), null, null, null));  // FIXME: type, label, and properties remain uninitialized
        }
        return result;
    }

    @Override
    public Topic createTopic(String typeId, Map properties) {
        Node node = graphDb.createNode();
        logger.info("Creating node, ID=" + node.getId());
        setNodeType(node, typeId);
        setProperties(node, properties);
        return new Topic(node.getId(), typeId, null, properties);  // FIXME: label remains uninitialized
    }

    @Override
    public void setTopicProperties(long id, Map properties) {
        logger.info("Setting properties of node " + id + ": " + properties.toString());
        Node node = graphDb.getNodeById(id);
        setProperties(node, properties);
    }

    @Override
    public void deleteTopic(long id) {
        logger.info("Deleting node " + id);
        Node node = graphDb.getNodeById(id);
        // delete relationships
        for (Relationship rel : node.getRelationships()) {
            rel.delete();
        }
        //
        node.delete();
    }

    // --- Relations ---

    @Override
    public Relation getRelation(long srcTopicId, long dstTopicId) {
        logger.info("Getting relationship between nodes " + srcTopicId + " and " + dstTopicId);
        Relationship relationship = null;
        Node node = graphDb.getNodeById(srcTopicId);
        for (Relationship rel : node.getRelationships()) {
            Node relNode = rel.getOtherNode(node);
            if (relNode.getId() == dstTopicId) {
                relationship = rel;
                break;
            }
        }
        if (relationship != null) {
            logger.info("=> relationship found (ID=" + relationship.getId() + ")");
            return new Relation(relationship.getId(), null, srcTopicId, dstTopicId, null);  // FIXME: typeId and properties remain uninitialized
        } else {
            logger.info("=> no such relationship");
            return null;
        }
    }

    @Override
    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties) {
        logger.info("Creating \"" + typeId + "\" relationship from node " + srcTopicId + " to " + dstTopicId);
        Node srcNode = graphDb.getNodeById(srcTopicId);
        Node dstNode = graphDb.getNodeById(dstTopicId);
        Relationship relationship = srcNode.createRelationshipTo(dstNode, RelType.valueOf(typeId));
        setProperties(relationship, properties);
        return new Relation(relationship.getId(), typeId, srcTopicId, dstTopicId, properties);
    }

    @Override
    public void deleteRelation(long id) {
        logger.info("Deleting relationship " + id);
        graphDb.getRelationshipById(id).delete();
    }

    // --- Types ---

    @Override
    public void createTopicType(Map<String, Object> properties, List<Map> dataFields) {
        Node type = graphDb.createNode();
        Object typeId = properties.get("type_id");
        logger.info("Creating topic type \"" + typeId + "\", ID=" + type.getId());
        setProperties(type, properties);
        index.index(type, "type_id", typeId);
        graphDb.getReferenceNode().createRelationshipTo(type, RelType.TOPIC_TYPE);
        //
        for (Map dataField : dataFields) {
            Node dataFieldNode = graphDb.createNode();
            setProperties(dataFieldNode, dataField);
            type.createRelationshipTo(dataFieldNode, RelType.DATA_FIELD);
        }
    }

    @Override
    public boolean topicTypeExists(String typeId) {
        return getNodeType(typeId) != null;
    }

    // --- Misc ---

    @Override
    public de.deepamehta.core.storage.Transaction beginTx() {
        return new Neo4jTransaction(graphDb);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down DB and indexing services");
        graphDb.shutdown();
        index.shutdown();
        fulltextIndex.shutdown();
    }



    // ***********************
    // *** Private Helpers ***
    // ***********************



    // --- Topics ---

    private Topic buildTopic(Node node) {
        String typeId = getTypeId(node);
        // label
        String label;
        TopicType topicType = topicTypes.get(typeId);
        String typeLabelField = topicType.getProperty("type_label_field");
        if (typeLabelField != null) {
            throw new RuntimeException("not yet implemented");
        } else {
            String fieldId = topicType.getDataField(0).get("field_id");
            label = (String) node.getProperty(fieldId);
        }
        //
        return new Topic(node.getId(), typeId, label, null);    // FIXME: properties remain uninitialized
    }

    private String getTypeId(Node node) {
        //
        if (node.getProperty("type_id", null) != null) {        // FIXME: recognize a type by incoming TOPIC_TYPE relation
            return "Topic Type";
        }
        //
        return (String) getNodeType(node).getProperty("type_id");
    }

    // --- Properties ---

    private Map getProperties(PropertyContainer container) {
        Map properties = new HashMap();
        for (String key : container.getPropertyKeys()) {
            properties.put(key, container.getProperty(key));
        }
        return properties;
    }

    private void setProperties(PropertyContainer container, Map<String, Object> properties) {
        if (properties == null) {
            throw new NullPointerException("setProperties() called with properties=null");
        }
        for (String key : properties.keySet()) {
            Object value = properties.get(key);
            container.setProperty(key, value);
            // fulltext index
            if (container instanceof Node) {
                fulltextIndex.index((Node) container, "default", value);
            }
        }
    }

    // --- Types ---

    private Node getNodeType(String typeId) {
        return index.getSingleNode("type_id", typeId);
    }

    private Node getNodeType(Node node) {
        Traverser traverser = node.traverse(Order.BREADTH_FIRST,
            StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE,
            RelType.INSTANCE, Direction.INCOMING);
        Iterator<Node> i = traverser.iterator();
        assert i.hasNext() : node;
        Node type = i.next();
        assert !i.hasNext() : node;
        return type;
    }

    private void setNodeType(Node node, String typeId) {
        Node type = getNodeType(typeId);
        assert type != null : "Topic type \"" + typeId + "\" not found in DB";
        type.createRelationshipTo(node, RelType.INSTANCE);
    }
}
