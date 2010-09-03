package de.deepamehta.core.service;

import de.deepamehta.core.model.Relation;
import de.deepamehta.core.model.Topic;

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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import java.io.InputStream;
import java.io.IOException;

import java.net.URL;



/**
 * Base class for plugin developers to derive their plugins from.
 */
public class Plugin implements BundleActivator {

    private String pluginId;                    // Bundle's symbolic name.
    private String pluginName;
    private String pluginClass;
    private String pluginPackage;
    private Bundle pluginBundle;
    private Topic  pluginTopic;                 // Represents this plugin in DB. Holds plugin migration number.

    protected Properties configProperties;      // Read from file "plugin.properties"
    private boolean isActivated;

    private ServiceTracker deepamehtaServiceTracker;
    protected static CoreService dms;

    private ServiceTracker httpServiceTracker;
    private HttpService httpService;

    private Logger logger = Logger.getLogger(getClass().getName());



    // ************************
    // *** Public Accessors ***
    // ************************



    public String getName() {
        return pluginName;
    }

    public Topic getPluginTopic() {
        return pluginTopic;
    }

    public String getConfigProperty(String key) {
        return getConfigProperty(key, null);
    }

    /**
     * Uses the plugin bundle's class loader to load the migration class for the given migration number.
     *
     * @return  the migration class, or <code>null</code> if there is no such class.
     *          Note: a migration can be purely declarative (just consisting of a typesNN.json file).
     */
    public Class getMigrationClass(int migrationNr) throws ClassNotFoundException,
                                                           InstantiationException,
                                                           IllegalAccessException {
        // Generic plugins (plugin bundles not containing a Plugin subclass) which provide migration classes
        // must set the "pluginPackage" config property. Otherwise the migration classes can't be located.
        if (pluginPackage.equals("de.deepamehta.core.service")) {
            return null;
        }
        //
        String migrationClassName = pluginPackage + ".migrations.Migration" + migrationNr;
        try {
            return pluginBundle.loadClass(migrationClassName);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Uses the plugin bundle's class loader to find a resource.
     *
     * @return  A InputStream object or null if no resource with this name is found.
     */
    public InputStream getResourceAsStream(String name) throws IOException {
        URL url = pluginBundle.getResource(name);
        if (url != null) {
            return url.openStream();
        } else {
            return null;
        }
    }

    // FIXME: drop method and make dms protected instead?
    public static CoreService getService() {
        // CoreService dms = (CoreService) deepamehtaServiceTracker.getService();
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
            //
            logger.info("---------- Starting DeepaMehta plugin bundle \"" + pluginName + "\" ----------");
            //
            configProperties = readConfigFile();
            pluginPackage = getConfigProperty("pluginPackage", getClass().getPackage().getName());
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

    public void preUpdateHook(Topic topic, Map<String, Object> newProperties) {
    }

    public void postUpdateHook(Topic topic, Map<String, Object> oldProperties) {
    }

    // ---

    public void providePropertiesHook(Topic topic) {
    }

    public void providePropertiesHook(Relation relation) {
    }

    // ---

    public JSONObject executeCommandHook(String command, Map params, Map<String, String> clientContext) {
        return null;
    }



    // ***********************
    // *** Private Helpers ***
    // ***********************



    private ServiceTracker createDeepamehtaServiceTracker(BundleContext context) {
        return new ServiceTracker(context, CoreService.class.getName(), null) {

            @Override
            public Object addingService(ServiceReference serviceRef) {
                logger.info("Adding DeepaMehta core service to plugin \"" + pluginName + "\"");
                dms = (CoreService) super.addingService(serviceRef);
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
            logger.info("Registering web resources of plugin \"" + pluginName + "\" at \"/" + pluginId + "\"");
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
            String namespace = getConfigProperty("restResourcesNamespace");
            if (namespace != null) {
                logger.info("Registering REST resources of plugin \"" + pluginName + "\" at namespace \"" +
                    namespace + "\"");
                // Generic plugins (plugin bundles not containing a Plugin subclass) which provide resource classes
                // must set the "pluginPackage" config property. Otherwise the resource classes can't be located.
                if (pluginPackage.equals("de.deepamehta.core.service")) {
                    throw new RuntimeException("Resource classes can't be located because plugin package is unknown " +
                        "(there is neither a Plugin subclass nor a \"pluginPackage\" config property)");
                }
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
        String namespace = getConfigProperty("restResourcesNamespace");
        if (namespace != null) {
            logger.info("Unregistering REST resources of plugin \"" + pluginName + "\"");
            httpService.unregister(namespace);
        }
    }

    // --- Config Properties ---

    private Properties readConfigFile() {
        try {
            Properties properties = new Properties();
            // We always use the plugin bundle's classloader to access the config file.
            // getClass().getResource() would fail for generic plugins (plugin bundles not containing a plugin
            // subclass) because the core bundle's classloader would be used and it has no access.
            URL url = pluginBundle.getResource("/plugin.properties");
            if (url != null) {
                InputStream in = url.openStream();
                logger.info("Reading plugin config file \"/plugin.properties\"");
                properties.load(in);
            } else {
                logger.info("No plugin config file found -- Using default configuration");
            }
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("Plugin config file can't be read", e);
        }
    }

    private String getConfigProperty(String key, String defaultValue) {
        return configProperties.getProperty(key, defaultValue);
    }

    // ---

    private void initPluginTopic() {
        pluginTopic = findPluginTopic();
        if (pluginTopic != null) {
            logger.info("No need to create topic for plugin \"" + pluginName + "\" (already exists)");
        } else {
            logger.info("Creating topic for plugin \"" + pluginName + "\"");
            Map properties = new HashMap();
            properties.put("de/deepamehta/core/property/PluginID", pluginId);
            properties.put("de/deepamehta/core/property/PluginMigrationNr", 0);
            // FIXME: clientContext=null
            pluginTopic = dms.createTopic("de/deepamehta/core/topictype/Plugin", properties, null);
        }
    }

    private Topic findPluginTopic() {
        return dms.getTopic("de/deepamehta/core/property/PluginID", pluginId);
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

    /**
     * Determines the migrations to be run for this plugin and run them.
     */
    private void runPluginMigrations() {
        int migrationNr = (Integer) pluginTopic.getProperty("de/deepamehta/core/property/PluginMigrationNr");
        int requiredMigrationNr = Integer.parseInt(getConfigProperty("requiredPluginMigrationNr", "0"));
        int migrationsToRun = requiredMigrationNr - migrationNr;
        logger.info("migrationNr=" + migrationNr + ", requiredMigrationNr=" + requiredMigrationNr +
            " => Running " + migrationsToRun + " plugin migrations");
        for (int i = migrationNr + 1; i <= requiredMigrationNr; i++) {
            dms.runPluginMigration(this, i);
        }
    }
}
