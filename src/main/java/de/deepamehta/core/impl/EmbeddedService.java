package de.deepamehta.core.impl;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.RelatedTopic;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.service.Migration;
import de.deepamehta.core.service.Plugin;
import de.deepamehta.core.storage.Storage;
import de.deepamehta.core.storage.Transaction;
import de.deepamehta.core.storage.neo4j.Neo4jStorage;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;



public class EmbeddedService implements DeepaMehtaService {

    private Map<String, Plugin> plugins = new HashMap();

    private Storage storage;

    private enum Hook {

        PRE_CREATE("preCreateHook", Topic.class, Map.class),
        POST_CREATE("postCreateHook", Topic.class, Map.class),
        PRE_UPDATE("preUpdateHook", Topic.class),
        POST_UPDATE("postUpdateHook", Topic.class),

        PROVIDE_TOPIC_PROPERTIES("providePropertiesHook", Topic.class),
        PROVIDE_RELATION_PROPERTIES("providePropertiesHook", Relation.class);

        private final String name;
        private final Class[] paramClasses;

        private Hook(String name, Class... paramClasses) {
            this.name = name;
            this.paramClasses = paramClasses;
        }
    }

    private Logger logger = Logger.getLogger(getClass().getName());

    public EmbeddedService() {
        try {
            openDB();
        } catch (Throwable e) {
            throw new RuntimeException("Database can't be opened", e);
        }
        //
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            initDB();
            runCoreMigrations();
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Database can't be initialized", e);
        } finally {
            tx.finish();
            if (ex != null) {
                closeDB();
                throw ex;
            }
        }
    }



    // ****************************************
    // *** DeepaMehtaService Implementation ***
    // ****************************************



    // --- Topics ---

    @Override
    public Topic getTopic(long id) {
        Topic topic = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            topic = storage.getTopic(id);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic " + id + " can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return topic;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public Topic getTopic(String key, Object value) {
        Topic topic = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            topic = storage.getTopic(key, value);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic can't be retrieved (tried by property \"" + key + "\"=" + value + ")", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return topic;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public Object getTopicProperty(long topicId, String key) {
        Object value = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            value = storage.getTopicProperty(topicId, key);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Property \"" + key + "\" of topic " + topicId + " can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return value;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public List<Topic> getTopics(String typeUri) {
        List<Topic> topics = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            topics = storage.getTopics(typeUri);
            //
            for (Topic topic : topics) {
                triggerHook(Hook.PROVIDE_TOPIC_PROPERTIES, topic);
            }
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topics of type \"" + typeUri + "\" can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return topics;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public List<RelatedTopic> getRelatedTopics(long topicId, List<String> includeTopicTypes,
                                                             List<String> includeRelTypes,
                                                             List<String> excludeRelTypes) {
        // set defaults
        if (includeTopicTypes == null) includeTopicTypes = new ArrayList();
        if (includeRelTypes == null) includeRelTypes = new ArrayList();
        if (excludeRelTypes == null) excludeRelTypes = new ArrayList();
        // error check
        if (!includeRelTypes.isEmpty() && !excludeRelTypes.isEmpty()) {
            throw new IllegalArgumentException("includeRelTypes and excludeRelTypes can not be used at the same time");
        }
        //
        List<RelatedTopic> relTopics = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            relTopics = storage.getRelatedTopics(topicId, includeTopicTypes, includeRelTypes, excludeRelTypes);
            //
            for (RelatedTopic relTopic : relTopics) {
                triggerHook(Hook.PROVIDE_TOPIC_PROPERTIES, relTopic.getTopic());
                triggerHook(Hook.PROVIDE_RELATION_PROPERTIES, relTopic.getRelation());
            }
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Related topics of topic " + topicId + " can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return relTopics;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public Topic searchTopics(String searchTerm, String fieldUri, boolean wholeWord, Map clientContext) {
        Topic resultTopic = null;
        Transaction tx = storage.beginTx();
        try {
            List<Topic> searchResult = storage.searchTopics(searchTerm, fieldUri, wholeWord);
            // create result topic (a bucket)
            Map properties = new HashMap();
            properties.put("http://www.deepamehta.de/core/property/SearchTerm", searchTerm);
            resultTopic = createTopic("http://www.deepamehta.de/core/topictype/SearchResult",
                properties, clientContext);
            // associate result topics
            logger.fine("Relating " + searchResult.size() + " result topics");
            for (Topic topic : searchResult) {
                logger.fine("Relating " + topic);
                createRelation("SEARCH_RESULT", resultTopic.id, topic.id, new HashMap());
            }
            //
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return resultTopic;
        }
    }

    @Override
    public Topic createTopic(String typeUri, Map properties, Map clientContext) {
        Topic topic = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            Topic t = new Topic(-1, typeUri, null, initProperties(properties, typeUri));
            //
            triggerHook(Hook.PRE_CREATE, t, clientContext);
            //
            topic = storage.createTopic(t.typeUri, t.getProperties());
            //
            triggerHook(Hook.POST_CREATE, topic, clientContext);
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic of type \"" + typeUri + "\" can't be created", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return topic;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void setTopicProperties(long id, Map properties) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.setTopicProperties(id, properties);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Properties of topic " + id + " can't be set (" + properties + ")", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    @Override
    public void deleteTopic(long id) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.deleteTopic(id);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic " + id + " can't be deleted", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    // --- Relations ---

    @Override
    public Relation getRelation(long id) {
        Relation relation = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            relation = storage.getRelation(id);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Relation " + id + " can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return relation;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public Relation getRelation(long srcTopicId, long dstTopicId) {
        Relation relation = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            relation = storage.getRelation(srcTopicId, dstTopicId);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Relation between topics " + srcTopicId +
                " and " + dstTopicId + " can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return relation;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties) {
        Relation relation = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            Relation rel = new Relation(-1, typeId, srcTopicId, dstTopicId, properties);
            //
            relation = storage.createRelation(rel.typeId, rel.srcTopicId, rel.dstTopicId, rel.properties);
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Relation of type \"" + typeId + "\" can't be created", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return relation;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void setRelationProperties(long id, Map properties) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.setRelationProperties(id, properties);
            tx.success();   
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Properties of relation " + id + " can't be set (" + properties + ")", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    @Override
    public void deleteRelation(long id) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.deleteRelation(id);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Relation " + id + " can't be deleted", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    // --- Types ---

    @Override
    public Set<String> getTopicTypeUris() {
        Set typeUris = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            typeUris = storage.getTopicTypeUris();
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic type URIs can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return typeUris;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public TopicType getTopicType(String typeUri) {
        TopicType topicType = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            topicType = storage.getTopicType(typeUri);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic type \"" + typeUri + "\" can't be retrieved", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return topicType;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void createTopicType(Map properties, List dataFields) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            TopicType topicType = new TopicType(properties, dataFields);
            //
            triggerHook(Hook.PRE_CREATE, topicType, null);  // FIXME: clientContext=null
            //
            storage.createTopicType(properties, dataFields);
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic type \"" +
                properties.get("http://www.deepamehta.de/core/property/TypeURI") + "\" can't be created", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    @Override
    public void addDataField(String typeUri, DataField dataField) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.addDataField(typeUri, dataField);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Data field \"" + dataField.uri + "\" can't be added to topic type \"" +
                typeUri + "\"", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    @Override
    public void updateDataField(String typeUri, DataField dataField) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.updateDataField(typeUri, dataField);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Data field \"" + dataField.uri + "\" of topic type \"" +
                typeUri + "\" can't be updated", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    public void removeDataField(String typeUri, String fieldUri) {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            storage.removeDataField(typeUri, fieldUri);
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Data field \"" + fieldUri + "\" of topic type \"" +
                typeUri + "\" can't be removed", e);
        } finally {
            tx.finish();
            if (ex != null) {
                throw ex;
            }
        }
    }

    // --- Plugins ---

    @Override
    public void registerPlugin(String pluginId, Plugin plugin) {
        plugins.put(pluginId, plugin);
    }

    @Override
    public void unregisterPlugin(String pluginId) {
        if (plugins.remove(pluginId) == null) {
            throw new RuntimeException("Plugin " + pluginId + " is not registered");
        }
    }

    @Override
    public Set<String> getPluginIds() {
        return plugins.keySet();
    }

    @Override
    public Plugin getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }

    @Override
    public void runPluginMigration(Plugin plugin, int migrationNr) throws RuntimeException {
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            Migration migration = plugin.getMigration(migrationNr);
            logger.info("Running plugin migration " + migration.getClass().getName());
            migration.setDeepaMehtaService(this);
            migration.run();
            // update DB model version
            logger.info("Plugin migration complete - Updating DB model version (" + migrationNr + ")");
            updatePluginDbModelVersion(plugin, migrationNr);
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Plugin migration can't run", e);
        } finally {
            tx.finish();
            if (ex != null) throw ex;
        }
    }

    // --- Misc ---

    @Override
    public void shutdown() {
        closeDB();
    }



    // ************************
    // *** Private Helpers ****
    // ************************



    // --- Topics ---

    // FIXME: method to be dropped. Missing properties should be regarded as normal state.
    // Otherwise all instances would be required to be updated once a data field has been added to the type definition.
    // Application logic (server-side) and also the client should cope with missing properties.
    private Map initProperties(Map properties, String typeUri) {
        if (properties == null) {
            properties = new HashMap();
        }
        for (DataField dataField : getTopicType(typeUri).getDataFields()) {
            if (!dataField.dataType.equals("relation") && properties.get(dataField.uri) == null) {
                properties.put(dataField.uri, "");
            }
        }
        return properties;
    }

    // --- Plugins ---

    private void triggerHook(Hook hook, Object... params) throws NoSuchMethodException,
                                                                 IllegalAccessException,
                                                                 InvocationTargetException {
        for (Plugin plugin : plugins.values()) {
            Method hookMethod = plugin.getClass().getMethod(hook.name, hook.paramClasses);
            hookMethod.invoke(plugin, params);
        }
    }

    private void updatePluginDbModelVersion(Plugin plugin, int dbModelVersion) {
        Map properties = new HashMap();
        properties.put("http://www.deepamehta.de/core/property/DBModelVersion", dbModelVersion);
        setTopicProperties(plugin.getPluginTopic().id, properties);
    }

    // --- DB ---

    private void openDB() {
        // FIXME: make the DB path a configuration setting
        storage = new Neo4jStorage("/Users/jri/var/db/deepamehta-db-neo4j");
    }

    private void initDB() {
        storage.init();
    }

    private void closeDB() {
        storage.shutdown();
    }

    //

    private void runCoreMigrations() {
        int dbModelVersion = storage.getDbModelVersion();
        int requiredDbModelVersion = REQUIRED_DB_MODEL_VERSION;
        int migrationsToRun = requiredDbModelVersion - dbModelVersion;
        logger.info("dbModelVersion=" + dbModelVersion + ", requiredDbModelVersion=" +
            requiredDbModelVersion + " => Running " + migrationsToRun + " core migrations");
        for (int i = dbModelVersion + 1; i <= requiredDbModelVersion; i++) {
            runCoreMigration(i);
        }
    }

    private void runCoreMigration(int migrationNr) {
        try {
            String migrationClassName = "de.deepamehta.core.migrations.Migration" + migrationNr;
            Migration migration = (Migration) Class.forName(migrationClassName).newInstance();
            logger.info("Running core migration " + migration.getClass().getName());
            migration.setDeepaMehtaService(this);
            migration.run();
            // update DB model version
            logger.info("Core migration complete - Updating DB model version (" + migrationNr + ")");
            storage.setDbModelVersion(migrationNr);
        } catch (Throwable e) {
            throw new RuntimeException("Core migration can't run", e);
        }
    }
}
