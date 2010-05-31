package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.impl.TypeCache;
import de.deepamehta.core.model.DataField;
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
import org.neo4j.meta.model.MetaModel;
import org.neo4j.meta.model.MetaModelClass;
import org.neo4j.meta.model.MetaModelImpl;
import org.neo4j.meta.model.MetaModelNamespace;
import org.neo4j.meta.model.MetaModelProperty;
import org.neo4j.meta.model.MetaModelRelTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;



public class Neo4jStorage implements Storage {

    private final Logger logger = Logger.getLogger(getClass().getName());

    private GraphDatabaseService graphDb;
    private IndexService index;
    private LuceneFulltextQueryIndexService fulltextIndex;
    private MetaModelNamespace namespace;
    //
    private final TypeCache typeCache;

    private enum RelType implements RelationshipType {
        RELATION, SEARCH_RESULT
    }

    public Neo4jStorage(String dbPath, TypeCache typeCache) {
        logger.info("Creating DB and indexing services");
        this.typeCache = typeCache;
        //
        graphDb = new EmbeddedGraphDatabase(dbPath);
    }



    // ******************************
    // *** Storage Implementation ***
    // ******************************



    // --- Topics ---

    @Override
    public Topic getTopic(long id) {
        logger.info("Getting node " + id);
        Node node = graphDb.getNodeById(id);
        // FIXME: label remains uninitialized
        return new Topic(id, getTypeId(node), null, getProperties(node));
    }

    @Override
    public Topic getTopic(String key, Object value) {
        logger.info("Getting node with " + key + "=" + value);
        Node node = index.getSingleNode(key, value);
        // FIXME: type and label remain uninitialized
        return node != null ? new Topic(node.getId(), null, null, getProperties(node)) : null;
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
        // setNodeType(node, typeId);
        getMetaClass(typeId).getDirectInstances().add(node);
        setProperties(node, properties, typeId);
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
    public Set<String> getTopicTypeIds() {
        Set typeIds = new HashSet();
        for (MetaModelClass metaClass : namespace.getMetaClasses()) {
            typeIds.add(metaClass.getName());
        }
        return typeIds;
    }

    @Override
    public void createTopicType(Map<String, Object> properties, List<DataField> dataFields) {
        String typeId = (String) properties.get("type_id");
        MetaModelClass metaClass = namespace.getMetaClass(typeId, true);
        logger.info("Creating topic type \"" + typeId + "\", ID=" + metaClass.node().getId());
        // setProperties(type, properties, "Topic Type");
        for (String key : properties.keySet()) {
            metaClass.node().setProperty(key, properties.get(key));
        }
        //
        // index.index(type, "type_id", typeId);
        // graphDb.getReferenceNode().createRelationshipTo(type, RelType.TOPIC_TYPE);
        //
        for (DataField dataField : dataFields) {
            addDataField(typeId, dataField);
        }
    }

    @Override
    public void addDataField(String typeId, DataField dataField) {
        Map<String, Object> properties = new HashMap();
        properties.put("id", dataField.id);
        properties.put("data_type", dataField.dataType);
        properties.put("editor", dataField.editor);
        properties.put("index_mode", dataField.indexMode);
        //
        // Node dataFieldNode = graphDb.createNode();
        MetaModelProperty metaProperty = namespace.getMetaProperty(dataField.id, true);
        logger.info("Creating data field \"" + dataField.id + "\", ID=" + metaProperty.node().getId());
        // setProperties(dataFieldNode, properties, "Data Field");
        for (String key : properties.keySet()) {
            metaProperty.node().setProperty(key, properties.get(key));
        }
        // getTypeNode(typeId).createRelationshipTo(dataFieldNode, RelType.DATA_FIELD);
        getMetaClass(typeId).getDirectProperties().add(metaProperty);
    }

    @Override
    public boolean topicTypeExists(String typeId) {
        return getMetaClass(typeId) != null;
    }

    // --- DB ---

    @Override
    public de.deepamehta.core.storage.Transaction beginTx() {
        return new Neo4jTransaction(graphDb);
    }

    /**
     * Performs storage layer initialization which is required to run in a transaction.
     */
    @Override
    public void setup() {
        // init index services
        index = new LuceneIndexService(graphDb);
        fulltextIndex = new LuceneFulltextQueryIndexService(graphDb);
        // init meta model
        MetaModel model = new MetaModelImpl(graphDb, index);
        namespace = model.getGlobalNamespace();
        // init DB model version
        if (!graphDb.getReferenceNode().hasProperty("db_model_version")) {
            logger.info("Starting with a fresh DB - Setting DB model version to 0");
            setDbModelVersion(0);
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutdown DB and indexing services");
        if (index != null) {
            index.shutdown();
            fulltextIndex.shutdown();
        } else {
            logger.warning("Indexing services not shutdown properly");
        }
        //
        graphDb.shutdown();
        graphDb = null;
    }

    @Override
    public int getDbModelVersion() {
        return (Integer) graphDb.getReferenceNode().getProperty("db_model_version");
    }

    @Override
    public void setDbModelVersion(int dbModelVersion) {
        graphDb.getReferenceNode().setProperty("db_model_version", dbModelVersion);
    }



    // ***********************
    // *** Private Helpers ***
    // ***********************



    // --- Topics ---

    private Topic buildTopic(Node node) {
        String typeId = getTypeId(node);
        // initialize label
        String label;
        TopicType topicType = typeCache.getTopicType(typeId);
        String typeLabelField = topicType.getProperty("label_field");
        if (typeLabelField != null) {
            throw new RuntimeException("not yet implemented");
        } else {
            String fieldId = topicType.getDataField(0).id;
            label = (String) node.getProperty(fieldId);
        }
        //
        return new Topic(node.getId(), typeId, label, null);    // FIXME: properties remain uninitialized
    }

    private String getTypeId(Node node) {
        // FIXME: meta-types must be detected manually
        if (node.getProperty("type_id", null) != null) {
            // FIXME: a more elaborated criteria is required, e.g. an incoming TOPIC_TYPE relation
            return "Topic Type";
        } else if (node.getProperty("data_type", null) != null) {
            // FIXME: a more elaborated criteria is required, e.g. an incoming DATA_FIELD relation
            return "Data Field";
        }
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
        String typeId = null;
        if (container instanceof Node) {
            typeId = getTypeId((Node) container);
        }
        setProperties(container, properties, typeId);
    }

    private void setProperties(PropertyContainer container, Map<String, Object> properties, String typeId) {
        if (properties == null) {
            throw new NullPointerException("setProperties() called with properties=null");
        }
        for (String key : properties.keySet()) {
            Object value = properties.get(key);
            // update DB
            container.setProperty(key, value);
            // update index
            indexProperty(container, key, value, typeId);
        }
    }

    private void indexProperty(PropertyContainer container, String key, Object value, String typeId) {
        // Note 1: we only index node properties. Neo4j can't index relationship properties.
        // Note 2: we only index instance nodes. Meta nodes (types and fields) are responsible for indexing themself.
        if (container instanceof Node && !typeId.equals("Topic Type") && !typeId.equals("Data Field")) {
            DataField dataField = typeCache.getTopicType(typeId).getDataField(key);
            String indexMode = dataField.indexMode;
            if ("off".equals(indexMode)) {
                return;
            } else if (indexMode == null || indexMode.equals("fulltext")) {
                fulltextIndex.index((Node) container, "default", value);
            } else if (indexMode.equals("id")) {
                index.index((Node) container, key, value);  // FIXME: include topic type in index key
            } else {
                throw new RuntimeException("Data field \"" + key + "\" of type definition \"" +
                    typeId + "\" has unexpectd index mode: \"" + indexMode + "\"");
            }
        }
    }

    // --- Types ---

    private Node getNodeType(Node node) {
        Traverser traverser = node.traverse(Order.BREADTH_FIRST,
            StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE,
            MetaModelRelTypes.META_HAS_INSTANCE, Direction.INCOMING);
        Iterator<Node> i = traverser.iterator();
        // error check 1
        if (!i.hasNext()) {
            throw new RuntimeException("Type of " + node + " is unknown " +
                "(there is no incoming META_HAS_INSTANCE relationship)");
        }
        //
        Node type = i.next();
        // error check 2
        if (i.hasNext()) {
            throw new RuntimeException("Type of " + node + " is ambiguous " +
                "(there are more than one incoming META_HAS_INSTANCE relationships)");
        }
        //
        return type;
    }

    private MetaModelClass getMetaClass(String typeId) {
        return namespace.getMetaClass(typeId, false);
    }
}
