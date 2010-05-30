package de.deepamehta.core.impl;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.model.TopicType;
import de.deepamehta.core.model.Relation;
import de.deepamehta.core.plugin.DeepaMehtaPlugin;
import de.deepamehta.core.plugin.Migration;
import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.storage.Storage;
import de.deepamehta.core.storage.Transaction;
import de.deepamehta.core.storage.neo4j.Neo4jStorage;

import org.osgi.framework.Bundle;

import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;

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

    private TypeCache typeCache = new TypeCache();
    private Map<String, DeepaMehtaPlugin> plugins = new HashMap();

    private Storage storage;

    private Logger logger = Logger.getLogger(getClass().getName());

    public EmbeddedService() {
        openDB();
        Transaction tx = storage.beginTx();
        try {
            setupDB();
            init();
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

    public List<Topic> getRelatedTopics(long topicId, List<String> excludeRelTypes) {
        List topics = null;
        Transaction tx = storage.beginTx();
        try {
            topics = storage.getRelatedTopics(topicId, excludeRelTypes);
            tx.success();   
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topics;
        }
    }

    public Topic searchTopics(String searchTerm) {
        Topic resultTopic = null;
        Transaction tx = storage.beginTx();
        try {
            List<Topic> searchResult = storage.searchTopics(searchTerm);
            // create result topic (a bucket)
            Map properties = new HashMap();
            properties.put("Search Term", searchTerm);
            resultTopic = createTopic("Search Result", properties);
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

    public Topic createTopic(String typeId, Map properties) {
        Topic topic = null;
        Transaction tx = storage.beginTx();
        try {
            Topic t = new Topic(-1, typeId, null, properties);
            triggerHook("preCreateHook", t);
            //
            topic = storage.createTopic(t.typeId, t.properties);
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
            return topic;
        }
    }

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

    public Collection<TopicType> getTopicTypes() {
        return typeCache.getTopicTypes();
    }

    public void createTopicType(Map properties, List dataFields) {
        Transaction tx = storage.beginTx();
        try {
            TopicType topicType = new TopicType(properties, dataFields);
            String typeId = topicType.getProperty("type_id");
            // update DB
            if (!topicTypeExists(typeId)) {
                storage.createTopicType(properties, dataFields);
            } else {
                logger.info("No need to create topic type \"" + typeId + "\" in DB (already exists)");
            }
            // update cache
            typeCache.addTopicType(topicType);
            //
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    public void addDataField(String typeId, DataField dataField) {
        Transaction tx = storage.beginTx();
        try {
            // update DB
            storage.addDataField(typeId, dataField);
            // update cache
            typeCache.getTopicType(typeId).addDataField(dataField);
            //
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    public boolean topicTypeExists(String typeId) {
        // Note: no transaction required because just the indexer is involved here
        return storage.topicTypeExists(typeId);
    }

    // --- Plugins ---

    public void registerPlugin(String pluginId, DeepaMehtaPlugin plugin) {
        plugins.put(pluginId, plugin);
    }

    public void unregisterPlugin(String pluginId) {
        if (plugins.remove(pluginId) == null) {
            throw new RuntimeException("Plugin " + pluginId + " is not registered");
        }
    }

    public Set<String> getPluginIds() {
        return plugins.keySet();
    }

    public DeepaMehtaPlugin getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }

    public void runMigration(DeepaMehtaPlugin plugin, int migrationNr) {
        Transaction tx = storage.beginTx();
        try {
            String migrationClassName = plugin.getClass().getPackage().getName() + ".migrations.Migration" + migrationNr;
            logger.info("Running migration " + migrationClassName);
            Class migrationClass = Class.forName(migrationClassName);
            Migration migration = (Migration) migrationClass.newInstance();
            migration.setDeepaMehtaService(this);
            migration.run();
            // update DB model version
            logger.info("Migration complete - Updating DB model version (" + migrationNr + ")");
            updatePluginDbModelVersion(plugin, migrationNr);
            //
            tx.success();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("ROLLBACK!");
        } finally {
            tx.finish();
        }
    }

    // --- Misc ---

    public void shutdown() {
        closeDB();
    }



    // ************************
    // *** Private Helpers ****
    // ************************



    // --- Plugins ---

    private void triggerHook(String hookName, Object... args) throws NoSuchMethodException,
                                                                     IllegalAccessException,
                                                                     InvocationTargetException {
        Class[] argClasses = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argClasses[i] = args[i].getClass();
        }
        //
        for (DeepaMehtaPlugin plugin : plugins.values()) {
            Method hook = plugin.getClass().getMethod(hookName, argClasses);
            hook.invoke(plugin, args);
        }
    }

    private void updatePluginDbModelVersion(DeepaMehtaPlugin plugin, int dbModelVersion) {
        Map properties = new HashMap();
        properties.put("db_model_version", dbModelVersion);
        setTopicProperties(plugin.getPluginTopic().id, properties);
    }

    // --- DB ---

    private void openDB() {
        storage = new Neo4jStorage("/Users/jri/var/db/deepamehta-db-neo4j", typeCache);
    }

    private void setupDB() {
        storage.setup();
    }

    private void closeDB() {
        storage.shutdown();
    }

    //

    private void init() {
        try {
            InputStream is = getClass().getResourceAsStream("/types.json");
            if (is == null) {
                throw new RuntimeException("Resource /types.json not found");
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(is));
            String line;
            StringBuilder json = new StringBuilder();
            while ((line = in.readLine()) != null) {
                json.append(line);
            }
            createTypes(json.toString());
        } catch (Throwable e) {
            throw new RuntimeException("ERROR while processing /types.json", e);
        }
    }

    private void createTypes(String json) throws JSONException {
        JSONArray types = new JSONArray(json);
        for (int i = 0; i < types.length(); i++) {
            TopicType topicType = new TopicType(types.getJSONObject(i));
            /* Bootstrap: meta-types must be in the cache before creating types
            if (topicType.getProperty("type_id").equals("Topic Type")) {
                typeCache.addTopicType(topicType);
            } */
            //
            createTopicType(topicType.properties, topicType.dataFields);
        }
    }
}
