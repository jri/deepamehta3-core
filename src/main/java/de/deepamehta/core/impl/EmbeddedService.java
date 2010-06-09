package de.deepamehta.core.impl;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.plugin.DeepaMehtaPlugin;
import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.service.Migration;
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

    private Map<String, DeepaMehtaPlugin> plugins = new HashMap();

    private Storage storage;

    private Logger logger = Logger.getLogger(getClass().getName());

    public EmbeddedService() {
        try {
            openDB();
        } catch (Throwable e) {
            throw new RuntimeException("Database can't be opened", e);
        }
        Transaction tx = storage.beginTx();
        try {
            setupDB();
            runCoreMigrations();
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
            closeDB();
        } finally {
            tx.finish();
        }
    }



    // ****************************************
    // *** DeepaMehtaService Implementation ***
    // ****************************************



    // --- Topics ---

    @Override
    public Topic getTopic(long id) {
        Topic topic = null;
        Transaction tx = storage.beginTx();
        try {
            topic = storage.getTopic(id);
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topic;
        }
    }

    @Override
    public Topic getTopic(String key, Object value) {
        Topic topic = null;
        Transaction tx = storage.beginTx();
        try {
            topic = storage.getTopic(key, value);
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topic;
        }
    }

    @Override
    public String getTopicProperty(long topicId, String key) {
        String value = null;
        Transaction tx = storage.beginTx();
        try {
            value = storage.getTopicProperty(topicId, key);
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return value;
        }
    }

    @Override
    public List<Topic> getTopics(String typeId) {
        List<Topic> topics = null;
        Transaction tx = storage.beginTx();
        try {
            topics = storage.getTopics(typeId);
            //
            for (Topic topic : topics) {
                triggerHook("provideData", topic);
            }
            //
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topics;
        }
    }

    @Override
    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes) {
        List<Topic> topics = null;
        Transaction tx = storage.beginTx();
        try {
            topics = storage.getRelatedTopics(topicId, excludeRelTypes);
            //
            for (Topic topic : topics) {
                triggerHook("provideData", topic);
            }
            //
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topics;
        }
    }

    @Override
    public Topic searchTopics(String searchTerm, String fieldId, boolean wholeWord) {
        Topic resultTopic = null;
        Transaction tx = storage.beginTx();
        try {
            List<Topic> searchResult = storage.searchTopics(searchTerm, fieldId, wholeWord);
            // create result topic (a bucket)
            Map properties = new HashMap();
            properties.put("Search Term", searchTerm);
            resultTopic = createTopic("Search Result", properties, null);
            // associate result topics
            for (Topic topic : searchResult) {
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
    public Topic createTopic(String typeId, Map properties, Map clientContext) {
        Topic newTopic = null;
        RuntimeException ex = null;
        Transaction tx = storage.beginTx();
        try {
            Topic topic = new Topic(-1, typeId, null, properties);
            //
            triggerHook("preCreateHook", topic, clientContext);
            //
            newTopic = storage.createTopic(topic.typeId, topic.properties);
            //
            triggerHook("postCreateHook", newTopic, clientContext);
            //
            tx.success();
        } catch (Throwable e) {
            logger.warning("ROLLBACK!");
            ex = new RuntimeException("Topic can't be created (type=" + typeId + ")", e);
        } finally {
            tx.finish();
            if (ex == null) {
                return newTopic;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void setTopicProperties(long id, Map properties) {
        Transaction tx = storage.beginTx();
        try {
            storage.setTopicProperties(id, properties);
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    @Override
    public void deleteTopic(long id) {
        Transaction tx = storage.beginTx();
        try {
            storage.deleteTopic(id);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    // --- Relations ---

    /**
     * Returns the relation between the two topics (regardless of type and direction).
     * If no such relation exists null is returned.
     * If more than one relation exists, only the first one is returned.
     */
    @Override
    public Relation getRelation(long srcTopicId, long dstTopicId) {
        Relation relation = null;
        Transaction tx = storage.beginTx();
        try {
            relation = storage.getRelation(srcTopicId, dstTopicId);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return relation;
        }
    }

    @Override
    public Relation createRelation(String typeId, long srcTopicId, long dstTopicId, Map properties) {
        Relation relation = null;
        Transaction tx = storage.beginTx();
        try {
            relation = storage.createRelation(typeId, srcTopicId, dstTopicId, properties);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return relation;
        }
    }

    @Override
    public void deleteRelation(long id) {
        Transaction tx = storage.beginTx();
        try {
            storage.deleteRelation(id);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    // --- Types ---

    @Override
    public Set<String> getTopicTypeIds() {
        Set typeIds = null;
        Transaction tx = storage.beginTx();
        try {
            typeIds = storage.getTopicTypeIds();
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return typeIds;
        }
    }

    @Override
    public TopicType getTopicType(String typeId) {
        TopicType topicType = null;
        Transaction tx = storage.beginTx();
        try {
            topicType = storage.getTopicType(typeId);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topicType;
        }
    }

    @Override
    public void createTopicType(Map properties, List dataFields) {
        Transaction tx = storage.beginTx();
        try {
            storage.createTopicType(properties, dataFields);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    @Override
    public void addDataField(String typeId, DataField dataField) {
        Transaction tx = storage.beginTx();
        try {
            storage.addDataField(typeId, dataField);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    // --- Plugins ---

    @Override
    public void registerPlugin(String pluginId, DeepaMehtaPlugin plugin) {
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
    public DeepaMehtaPlugin getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }

    @Override
    public void runPluginMigration(DeepaMehtaPlugin plugin, int migrationNr) throws RuntimeException {
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



    // --- Plugins ---

    private void triggerHook(String hookName, Object... params) throws NoSuchMethodException,
                                                                       IllegalAccessException,
                                                                       InvocationTargetException {
        Class[] paramClasses = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            if (params[i] == null) {
                throw new NullPointerException("Parameter " + i + " of hook call \"" + hookName + "\" is null");
            }
            paramClasses[i] = params[i].getClass();
        }
        //
        for (DeepaMehtaPlugin plugin : plugins.values()) {
            Method hook = plugin.getClass().getMethod(hookName, paramClasses);
            hook.invoke(plugin, params);
        }
    }

    private void updatePluginDbModelVersion(DeepaMehtaPlugin plugin, int dbModelVersion) {
        Map properties = new HashMap();
        properties.put("db_model_version", String.valueOf(dbModelVersion));
        setTopicProperties(plugin.getPluginTopic().id, properties);
    }

    // --- DB ---

    private void openDB() {
        // FIXME: make the DB path a configuration setting
        storage = new Neo4jStorage("/Users/jri/var/db/deepamehta-db-neo4j");
    }

    private void setupDB() {
        storage.setup();
    }

    private void closeDB() {
        storage.shutdown();
    }

    //

    private void runCoreMigrations() throws Exception {
        int dbModelVersion = storage.getDbModelVersion();
        int requiredDbModelVersion = REQUIRED_DB_MODEL_VERSION;
        int migrationsToRun = requiredDbModelVersion - dbModelVersion;
        logger.info("dbModelVersion=" + dbModelVersion + ", requiredDbModelVersion=" +
            requiredDbModelVersion + " => Running " + migrationsToRun + " core migrations");
        for (int i = dbModelVersion + 1; i <= requiredDbModelVersion; i++) {
            runCoreMigration(i);
        }
    }

    private void runCoreMigration(int migrationNr) throws Exception {
        String migrationClassName = "de.deepamehta.core.migrations.Migration" + migrationNr;
        Migration migration = (Migration) Class.forName(migrationClassName).newInstance();
        logger.info("Running core migration " + migration.getClass().getName());
        migration.setDeepaMehtaService(this);
        migration.run();
        // update DB model version
        logger.info("Core migration complete - Updating DB model version (" + migrationNr + ")");
        storage.setDbModelVersion(migrationNr);
    }
}
