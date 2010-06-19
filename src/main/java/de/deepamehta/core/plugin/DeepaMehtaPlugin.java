package de.deepamehta.core.plugin;

import de.deepamehta.core.model.Relation;
import de.deepamehta.core.model.Topic;
import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.service.Migration;

import com.sun.jersey.spi.container.servlet.ServletContainer;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.service.http.HttpContext;
import org.osgi.service.http.HttpService;
import org.osgi.service.http.NamespaceException;
import org.osgi.util.tracker.ServiceTracker;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import java.io.InputStream;
import java.io.IOException;



/**
 * Base class for plugin developers to derive their plugins from.
 */
public class DeepaMehtaPlugin implements BundleActivator {

    private String     pluginId;
    private String     pluginName;
    private String     pluginClass;
    private String     pluginPackage;
    private Bundle     pluginBundle;
    private Topic      pluginTopic;         // Represents this plugin in DB. Holds plugin DB version number.
    private Properties pluginProperties;    // Read from plugin.properties

    private boolean isActivated;

    private ServiceTracker deepamehtaServiceTracker;
    protected static DeepaMehtaService dms;

    private ServiceTracker httpServiceTracker;
    private HttpService httpService;

    private Logger logger = Logger.getLogger(getClass().getName());



    // ************************
    // *** Public Accessors ***
    // ************************



    public Topic getPluginTopic() {
        return pluginTopic;
    }

    public String getProperty(String key) {
        return getProperty(key, null);
    }

    public Migration getMigration(int migrationNr) throws ClassNotFoundException,
                                                          InstantiationException,
                                                          IllegalAccessException {
        String migrationClassName = pluginPackage + ".migrations.Migration" + migrationNr;
        return (Migration) pluginBundle.loadClass(migrationClassName).newInstance();
    }

    // FIXME: drop method and make dms protected instead?
    public static DeepaMehtaService getService() {
        // DeepaMehtaService dms = (DeepaMehtaService) deepamehtaServiceTracker.getService();
        if (dms == null) {
            throw new RuntimeException("DeepaMehta core service is currently not available");
        }
        return dms;
    }



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    public void start(BundleContext context) {
        try {
            pluginBundle = context.getBundle();
            pluginId = pluginBundle.getSymbolicName();
            pluginName = (String) pluginBundle.getHeaders().get("Bundle-Name");
            pluginClass = (String) pluginBundle.getHeaders().get("Bundle-Activator");
            pluginPackage = getClass().getPackage().getName();
            //
            logger.info("---------- Starting DeepaMehta plugin bundle \"" + pluginName + "\" ----------");
            //
            pluginProperties = readProperties();
            //
            deepamehtaServiceTracker = createDeepamehtaServiceTracker(context);
            deepamehtaServiceTracker.open();
            //
            httpServiceTracker = createHttpServiceTracker(context);
            httpServiceTracker.open();
        } catch (RuntimeException e) {
            logger.severe("Plugin \"" + pluginName + "\" can't be activated. Reason:");
            e.printStackTrace();
            throw e;
        }
    }

    public void stop(BundleContext context) {
        logger.info("---------- Stopping DeepaMehta plugin bundle \"" + pluginName + "\" ----------");
        //
        deepamehtaServiceTracker.close();
        httpServiceTracker.close();
    }



    // *************
    // *** Hooks ***
    // *************



    public void preCreateHook(Topic topic, Map<String, String> clientContext) {
    }

    public void postCreateHook(Topic topic, Map<String, String> clientContext) {
    }

    public void preUpdateHook(Topic topic) {
    }

    public void postUpdateHook(Topic topic) {
    }

    // ---

    public void providePropertiesHook(Topic topic) {
    }

    public void providePropertiesHook(Relation relation) {
    }



    // ***********************
    // *** Private Helpers ***
    // ***********************



    private ServiceTracker createDeepamehtaServiceTracker(BundleContext context) {
        return new ServiceTracker(context, DeepaMehtaService.class.getName(), null) {

            @Override
            public Object addingService(ServiceReference serviceRef) {
                logger.info("Adding DeepaMehta core service to plugin \"" + pluginName + "\"");
                dms = (DeepaMehtaService) super.addingService(serviceRef);
                initPlugin();
                return dms;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == dms) {
                    logger.info("Removing DeepaMehta core service from plugin \"" + pluginName + "\"");
                    unregisterPlugin();
                    dms = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    private ServiceTracker createHttpServiceTracker(BundleContext context) {
        return new ServiceTracker(context, HttpService.class.getName(), null) {

            @Override
            public Object addingService(ServiceReference serviceRef) {
                logger.info("Adding HTTP service to plugin \"" + pluginName + "\"");
                httpService = (HttpService) super.addingService(serviceRef);
                registerWebResources();
                registerRestResources();
                return httpService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == httpService) {
                    logger.info("Removing HTTP service from plugin \"" + pluginName + "\"");
                    unregisterWebResources();
                    unregisterRestResources();
                    httpService = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    // ---

    private void registerPlugin() {
        logger.info("Registering plugin \"" + pluginName + "\" at DeepaMehta core service");
        dms.registerPlugin(pluginId, this);
        isActivated = true;
    }

    private void unregisterPlugin() {
        if (isActivated) {
            logger.info("Unregistering plugin \"" + pluginName + "\" at DeepaMehta core service");
            dms.unregisterPlugin(pluginId);
        }
    }

    // ---

    private void registerWebResources() {
        try {
            logger.info("Registering web resources of plugin \"" + pluginName + "\"");
            httpService.registerResources("/" + pluginId, "/web", null);
        } catch (NamespaceException e) {
            throw new RuntimeException("Web resources of plugin \"" + pluginName + "\" can't be registered", e);
        }
    }

    private void unregisterWebResources() {
        logger.info("Unregistering web resources of plugin \"" + pluginName + "\"");
        httpService.unregister("/" + pluginId);
    }

    // ---

    private void registerRestResources() {
        try {
            String namespace = getProperty("restResourcesNamespace");
            if (namespace != null) {
                logger.info("Registering REST resources of plugin \"" + pluginName + "\" at namespace \"" +
                    namespace + "\"");
                //
                Dictionary initParams = new Hashtable();
                initParams.put("com.sun.jersey.config.property.packages", pluginPackage + ".resources");
            	//
                httpService.registerServlet(namespace, new ServletContainer(), initParams, null);
            }
        } catch (Exception e) {
            throw new RuntimeException("REST resources of plugin \"" + pluginName + "\" can't be registered", e);
        }
    }

    private void unregisterRestResources() {
        String namespace = getProperty("restResourcesNamespace");
        if (namespace != null) {
            logger.info("Unregistering REST resources of plugin \"" + pluginName + "\"");
            httpService.unregister(namespace);
        }
    }

    // ---

    private Properties readProperties() {
        try {
            Properties properties = new Properties();
            InputStream in = getClass().getResourceAsStream("/plugin.properties");
            if (in != null) {
                logger.info("Reading plugin properties from file /plugin.properties");
                properties.load(in);
            } else {
                logger.info("No plugin properties file available - Using default plugin properties");
            }
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("Plugin properties file can't be read", e);
        }
    }

    private String getProperty(String key, String defaultValue) {
        return pluginProperties.getProperty(key, defaultValue);
    }

    // ---

    private void initPluginTopic() {
        pluginTopic = findPluginTopic();
        if (pluginTopic != null) {
            logger.info("No need to create topic for plugin \"" + pluginName + "\" (already exists)");
        } else {
            logger.info("Creating topic for plugin \"" + pluginName + "\"");
            Map properties = new HashMap();
            properties.put("plugin_id", pluginId);
            properties.put("db_model_version", "0");
            pluginTopic = dms.createTopic("Plugin", properties, null);     // FIXME: clientContext=null
        }
    }

    private Topic findPluginTopic() {
        return dms.getTopic("plugin_id", pluginId);
    }

    // ---

    private void initPlugin() {
        try {
            logger.info("----- Initializing plugin \"" + pluginName + "\" -----");
            initPluginTopic();
            runPluginMigrations();
            registerPlugin();
        } catch (RuntimeException e) {
            logger.severe("Plugin \"" + pluginName + "\" can't be activated. Reason:");
            throw e;
        }
    }

    private void runPluginMigrations() {
        int dbModelVersion = Integer.parseInt(pluginTopic.getProperty("db_model_version"));
        int requiredPluginDbVersion = Integer.parseInt(getProperty("requiredPluginDBVersion", "0"));
        int migrationsToRun = requiredPluginDbVersion - dbModelVersion;
        logger.info("dbModelVersion=" + dbModelVersion + ", requiredPluginDbVersion=" + requiredPluginDbVersion +
            " => Running " + migrationsToRun + " plugin migrations");
        for (int i = dbModelVersion + 1; i <= requiredPluginDbVersion; i++) {
            dms.runPluginMigration(this, i);
        }
    }
}
