package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.storage.Storage;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.ReturnFilter;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.TraversalFactory;
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
import java.util.Collection;
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
    private MetaModelNamespace namespace;
    private IndexService index;
    private LuceneFulltextQueryIndexService fulltextIndex;
    //
    private final TypeCache typeCache;

    // SEARCH_RESULT relations are not part of the knowledge base but help to visualize / navigate result sets.
    static enum RelType implements RelationshipType {
        RELATION, SEARCH_RESULT,
        SEQUENCE_START, SEQUENCE
    }

    public Neo4jStorage(String dbPath) {
        logger.info("Creating DB and indexing services");
        this.typeCache = new TypeCache(this);
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
        return new Topic(id, getTypeId(node), null, getProperties(node));   // FIXME: label remains uninitialized
    }

    @Override
    public Topic getTopic(String key, Object value) {
        logger.info("Getting node by property (" + key + "=" + value + ")");
        Node node = index.getSingleNode(key, value);
        // FIXME: type and label remain uninitialized
        return node != null ? new Topic(node.getId(), null, null, getProperties(node)) : null;
    }

    @Override
    public Object getTopicProperty(long topicId, String key) {
        return graphDb.getNodeById(topicId).getProperty(key, null);
    }

    @Override
    public List<Topic> getTopics(String typeId) {
        List topics = new ArrayList();
        for (Node node : getMetaClass(typeId).getDirectInstances()) {
            topics.add(buildTopic(node));
        }
        return topics;
    }

    @Override
    public List<RelatedTopic> getRelatedTopics(long topicId, List<String> includeTopicTypes,
                                                             List<String> includeRelTypes,
                                                             List<String> excludeRelTypes) {
        // Note: we must exclude the meta-model's namespace and property nodes. They are not intended for
        // being exposed to the user (additionally, getTypeNode() would fail on these nodes).
        excludeRelTypes.add("META_CLASS;OUTGOING");
        excludeRelTypes.add("META_HAS_PROPERTY;INCOMING");
        TraversalDescription desc = createRelatedTopicsTraversalDescription(includeTopicTypes,
                                                                            includeRelTypes,
                                                                            excludeRelTypes);
        List relTopics = new ArrayList();
        Node startNode = graphDb.getNodeById(topicId);
        for (Position pos : desc.traverse(startNode)) {
            RelatedTopic relTopic = new RelatedTopic();
            relTopic.setTopic(buildTopic(pos.node()));
            // Note: the relation properties remain uninitialzed here.
            // It is up to the plugins to provide selected properties (see providePropertiesHook()).
            relTopic.setRelation(buildRelation(pos.lastRelationship(), false));
            relTopics.add(relTopic);
        }
        logger.info("=> " + relTopics.size() + " related nodes");
        return relTopics;
    }

    @Override
    public List<Topic> searchTopics(String searchTerm, String fieldId, boolean wholeWord) {
        if (fieldId == null) fieldId = "default";
        if (!wholeWord) searchTerm += "*";
        IndexHits<Node> hits = fulltextIndex.getNodes(fieldId, searchTerm);
        logger.info("Searching \"" + searchTerm + "\" in field \"" + fieldId + "\" => " + hits.size() + " nodes");
        List result = new ArrayList();
        for (Node node : hits) {
            logger.fine("Adding node " + node.getId());
            // Filter result set. Note: a search should not find other searches.
            //
            // TODO: drop this filter. Items not intended for being find should not be indexed at all. Model change
            // required: the indexing mode must be specified per topic type/data field pair instead per data field.
            if (!getTypeId(node).equals("Search Result")) {
                // FIXME: type, label, and properties remain uninitialized
                result.add(new Topic(node.getId(), null, null, null));
            }
        }
        logger.info("After filtering => " + result.size() + " nodes");
        return result;
    }

    @Override
    public Topic createTopic(String typeId, Map properties) {
        Node node = graphDb.createNode();
        logger.info("Creating node => ID=" + node.getId());
        // setNodeType(node, typeId);
        getMetaClass(typeId).getDirectInstances().add(node);
        setProperties(node, properties, typeId);
        return new Topic(node.getId(), typeId, null, properties);  // FIXME: label remains uninitialized
    }

    @Override
    public void setTopicProperties(long id, Map properties) {
        logger.info("Setting properties of node " + id + ": " + properties);
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
    public Relation getRelation(long id) {
        logger.info("Getting relationship " + id);
        Relationship relationship = graphDb.getRelationshipById(id);
        return buildRelation(relationship, true);
    }

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
            return buildRelation(relationship, true);
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
        Relationship relationship = srcNode.createRelationshipTo(dstNode, getRelationshipType(typeId));
        setProperties(relationship, properties);
        return new Relation(relationship.getId(), typeId, srcTopicId, dstTopicId, properties);
    }

    @Override
    public void setRelationProperties(long id, Map properties) {
        logger.info("Setting properties of relationship " + id + ": " + properties);
        Relationship relationship = graphDb.getRelationshipById(id);
        setProperties(relationship, properties);
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
        for (MetaModelClass metaClass : getAllMetaClasses()) {
            typeIds.add(metaClass.getName());
        }
        return typeIds;
    }

    @Override
    public TopicType getTopicType(String typeId) {
        return typeCache.get(typeId);
    }

    @Override
    public void createTopicType(Map<String, Object> properties, List<DataField> dataFields) {
        typeCache.put(new Neo4jTopicType(properties, dataFields, this));
    }

    @Override
    public void addDataField(String typeId, DataField dataField) {
        typeCache.get(typeId).addDataField(dataField);
    }

    @Override
    public void updateDataField(String typeId, DataField dataField) {
        typeCache.get(typeId).getDataField(dataField.id).update(dataField.getProperties());
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
    public void init() {
        // 1) init indexing services
        index = new LuceneIndexService(graphDb);
        fulltextIndex = new LuceneFulltextQueryIndexService(graphDb);
        // 2) init meta model
        MetaModel model = new MetaModelImpl(graphDb, index);
        namespace = model.getGlobalNamespace();
        // 3) init DB model version
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
    // *** Package Helpers ***
    // ***********************



    // --- Topics ---

    private Topic buildTopic(Node node) {
        // initialize type
        String typeId = getTypeId(node);
        // initialize label
        String label;
        TopicType topicType = typeCache.get(typeId);
        String typeLabelField = (String) topicType.getProperty("label_field", null);
        if (typeLabelField != null) {
            throw new RuntimeException("not yet implemented");
        } else {
            String fieldId = topicType.getDataField(0).id;
            label = node.getProperty(fieldId).toString();   // Note: property value can be a number as well
        }
        // Note: the properties remain uninitialzed here.
        // It is up to the plugins to provide selected properties (see providePropertiesHook()).
        return new Topic(node.getId(), typeId, label, null);
    }

    // --- Relations ---

    /**
     * Builds a DeepaMehta relation from a Neo4j relationship.
     *
     * @param   includeProperties   if true, the relation properties are fetched.
     */
    private Relation buildRelation(Relationship rel, boolean includeProperties) {
        Map properties = includeProperties ? getProperties(rel) : null;
        return new Relation(rel.getId(), rel.getType().name(),
            rel.getStartNode().getId(), rel.getEndNode().getId(), properties);
    }

    // --- Properties ---

    Map getProperties(PropertyContainer container) {
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
            if (container instanceof Node) {
                // Note: we only index node properties.
                // Neo4j can't index relationship properties.
                indexProperty((Node) container, key, value, typeId);
            }
        }
    }

    private void indexProperty(Node node, String key, Object value, String typeId) {
        // Note: we only index instance nodes. Meta nodes (types and fields) are responsible for indexing themself.
        if (!typeId.equals("Topic Type") && !typeId.equals("Data Field")) {
            DataField dataField = typeCache.get(typeId).getDataField(key);
            String indexingMode = dataField.indexingMode;
            if (indexingMode.equals("OFF")) {
                return;
            } else if (indexingMode.equals("KEY")) {
                index.index(node, key, value);  // FIXME: include topic type in index key
            } else if (indexingMode.equals("FULLTEXT")) {
                fulltextIndex.index(node, "default", value);
            } else if (indexingMode.equals("FULLTEXT_KEY")) {
                fulltextIndex.index(node, key, value);
            } else {
                throw new RuntimeException("Data field \"" + key + "\" of type definition \"" +
                    typeId + "\" has unexpectd indexing mode: \"" + indexingMode + "\"");
            }
        }
    }

    // --- Types ---

    private String getTypeId(Node node) {
        // FIXME: meta-types must be detected manually
        if (node.getProperty("type_id", null) != null) {
            // FIXME: a more elaborated criteria is required, e.g. an incoming TOPIC_TYPE relation
            return "Topic Type";
        // FIXME: drop "Data Field" detection?
        /* } else if (node.getProperty("data_type", null) != null) {
            // FIXME: a more elaborated criteria is required, e.g. an incoming DATA_FIELD relation
            return "Data Field"; */
        }
        return (String) getTypeNode(node).getProperty("type_id");
    }

    private Node getTypeNode(Node node) {
        TraversalDescription desc = TraversalFactory.createTraversalDescription();
        desc = desc.relationships(MetaModelRelTypes.META_HAS_INSTANCE, Direction.INCOMING);
        desc = desc.filter(ReturnFilter.ALL_BUT_START_NODE);
        //
        Iterator<Node> i = desc.traverse(node).nodes().iterator();
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

    // ---

    private RelationshipType getRelationshipType(String typeId) {
        try {
            // 1st try: static types are returned directly
            return RelType.valueOf(typeId);
        } catch (IllegalArgumentException e) {
            // 2nd try: search through dynamic types
            for (RelationshipType relType : graphDb.getRelationshipTypes()) {
                if (relType.name().equals(typeId)) {
                    return relType;
                }
            }
            // Last resort: create new type
            logger.info("### Relation type \"" + typeId + "\" does not exist - Creating it dynamically");
            return DynamicRelationshipType.withName(typeId);
        }
    }

    // --- Meta Model ---

    MetaModelClass getMetaClass(String typeId) {
        MetaModelClass metaClass = namespace.getMetaClass(typeId, false);
        if (metaClass == null) {
            throw new RuntimeException("Topic type \"" + typeId + "\" is unknown");
        }
        return metaClass;
    }

    Collection<MetaModelClass> getAllMetaClasses() {
        return namespace.getMetaClasses();
    }

    MetaModelClass createMetaClass(String typeId) {
        return namespace.getMetaClass(typeId, true);
    }

    MetaModelProperty createMetaProperty(String dataFieldId) {
        return namespace.getMetaProperty(dataFieldId, true);
    }

    // --- Traversal ---

    private TraversalDescription createRelatedTopicsTraversalDescription(List<String> includeTopicTypes,
                                                                         List<String> includeRelTypes,
                                                                         List<String> excludeRelTypes) {
        TraversalDescription desc = TraversalFactory.createTraversalDescription();
        desc = desc.filter(new RelatedTopicsFilter(includeTopicTypes, includeRelTypes, excludeRelTypes));
        desc = desc.prune(new DepthOnePruneEvaluator());
        return desc;
    }

    private class RelatedTopicsFilter implements ReturnFilter {

        private List<String> includeTopicTypes;
        private Map<String, Direction> includeRelTypes;
        private Map<String, Direction> excludeRelTypes;

        private RelatedTopicsFilter(List<String> includeTopicTypes,
                                    List<String> includeRelTypes, List<String> excludeRelTypes) {
            //
            this.includeTopicTypes = includeTopicTypes;
            this.includeRelTypes = parseRelTypeFilter(includeRelTypes);
            this.excludeRelTypes = parseRelTypeFilter(excludeRelTypes);
            //
        }

        @Override
        public boolean shouldReturn(Position position) {
            if (position.atStartNode()) {
                return false;
            }
            // Note: we must apply the relation type filter first in order to sort out the meta-model's namespace and
            // property nodes before the topic type filter would see them (getTypeId() would fail on these nodes).
            Node node = position.node();
            // 1) apply relation type filter
            Relationship rel = position.lastRelationship();
            String relTypeName = rel.getType().name();
            // apply include filter
            if (!includeRelTypes.isEmpty()) {
                Direction dir = includeRelTypes.get(relTypeName);
                if (dir == null || !directionMatches(node, rel, dir)) {
                    return false;
                }
            } else {
                // apply exclude filter
                Direction dir = excludeRelTypes.get(relTypeName);
                if (dir != null && directionMatches(node, rel, dir)) {
                    return false;
                }
            }
            // 2) apply topic type filter
            if (!includeTopicTypes.isEmpty() && !includeTopicTypes.contains(getTypeId(node))) {
                return false;
            }
            //
            return true;
        }

        // ---

        private Map parseRelTypeFilter(List<String> relTypes) {
            Map relTypeFilter = new HashMap();
            for (String relFilter : relTypes) {
                String[] relFilterTokens = relFilter.split(";");
                String relTypeName = relFilterTokens[0];
                Direction dir;
                if (relFilterTokens.length == 1) {
                    dir = Direction.BOTH;
                } else {
                    dir = Direction.valueOf(relFilterTokens[1]);
                }
                relTypeFilter.put(relTypeName, dir);
            }
            return relTypeFilter;
        }

        /**
         * Returns true if the relationship has the given direction from the perspective of the node.
         * Prerequisite: the given node is involved in the given relationship.
         */
        private boolean directionMatches(Node node, Relationship rel, Direction dir) {
            if (dir == Direction.BOTH) {
                return true;
            } if (dir == Direction.OUTGOING && node.equals(rel.getStartNode())) {
                return true;
            } else if (dir == Direction.INCOMING && node.equals(rel.getEndNode())) {
                return true;
            } else {
                return false;
            }
        }
    }

    private class DepthOnePruneEvaluator implements PruneEvaluator {

        @Override
        public boolean pruneAfter(Position position) {
            return position.depth() == 1;
        }
    }
}
