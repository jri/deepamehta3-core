package de.deepamehta.core.plugin;

import de.deepamehta.core.model.Topic;
import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.service.Migration;

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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;



public class DeepaMehtaPlugin implements BundleActivator {

    private String pluginId;
    private String pluginName;
    private String pluginClass;
    private Bundle pluginBundle;
    private Topic  pluginTopic;

    private boolean isActivated;

    private ServiceTracker deepamehtaServiceTracker;
    protected DeepaMehtaService dms;

    private ServiceTracker httpServiceTracker;
    private HttpService httpService;

    private Logger logger = Logger.getLogger(getClass().getName());



    // ****************
    // *** Accessor ***
    // ****************



    public Topic getPluginTopic() {
        return pluginTopic;
    }

    public Migration getMigration(int migrationNr) throws ClassNotFoundException,
                                                          InstantiationException,
                                                          IllegalAccessException {
        String migrationClassName = getClass().getPackage().getName() + ".migrations.Migration" + migrationNr;
        return (Migration) pluginBundle.loadClass(migrationClassName).newInstance();
    }

    // ### FIXME: drop method and make dms protected instead?
    protected DeepaMehtaService getService() {
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
        pluginBundle = context.getBundle();
        pluginId = pluginBundle.getSymbolicName();
        pluginName = (String) pluginBundle.getHeaders().get("Bundle-Name");
        pluginClass = (String) pluginBundle.getHeaders().get("Bundle-Activator");
        //
        logger.info("----- Starting DeepaMehta plugin bundle \"" + pluginName + "\" -----");
        //
        deepamehtaServiceTracker = createDeepamehtaServiceTracker(context);
        deepamehtaServiceTracker.open();
        //
        httpServiceTracker = createHttpServiceTracker(context);
        httpServiceTracker.open();
    }

    public void stop(BundleContext context) {
        logger.info("----- Stopping DeepaMehta plugin bundle \"" + pluginName + "\" -----");
        //
        deepamehtaServiceTracker.close();
        httpServiceTracker.close();
    }



    // *************
    // *** Hooks ***
    // *************



    public int requiredDBModelVersion() {
        return 0;
    }

    public String getClientPlugin() {
        return null;
    }

    // ---

    // Note: HashMap is used instead of Map in order to let our simple hook reflection mechanism
    // find this method. See EmbeddedService.triggerHook()
    public void preCreateHook(Topic topic, HashMap clientContext) {
    }

    // Note: HashMap is used instead of Map in order to let our simple hook reflection mechanism
    // find this method. See EmbeddedService.triggerHook()
    public void postCreateHook(Topic topic, HashMap clientContext) {
    }

    public void preUpdateHook(Topic topic) {
    }

    public void postUpdateHook(Topic topic) {
    }

    // ---

    public void provideData(Topic topic) {
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
                registerResources();
                return httpService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == httpService) {
                    logger.info("Removing HTTP service from plugin \"" + pluginName + "\"");
                    unregisterResources();
                    httpService = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    // ---

    private void registerPlugin() {
        logger.info("Registering plugin \"" + pluginName + "\" (" + pluginClass + ")");
        dms.registerPlugin(pluginId, this);
        isActivated = true;
    }

    private void unregisterPlugin() {
        if (isActivated) {
            logger.info("Unregistering plugin \"" + pluginName + "\" (" + pluginClass + ")");
            dms.unregisterPlugin(pluginId);
        }
    }

    // ---

    private void registerResources() {
        logger.info("Registering web resources of plugin \"" + pluginName + "\"");
        try {
            // Note: to map the bundle root, according to OSGi API the resource name "/" is to be used.
            // This doesn't work: java.lang.IllegalArgumentException: Malformed resource name [/]
            // Using "" instead works. IMO this is an error in the "Apache Felix Http Jetty" bundle.
            httpService.registerResources("/" + pluginId, "", null);
        } catch (NamespaceException e) {
            throw new RuntimeException(e);
        }
    }

    private void unregisterResources() {
        logger.info("Unregistering web resources of plugin \"" + pluginName + "\"");
        httpService.unregister("/" + pluginId);
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
            pluginTopic = dms.createTopic("Plugin", properties, new HashMap());     // FIXME: clientContext is empty
        }
    }

    private Topic findPluginTopic() {
        return dms.getTopic("plugin_id", pluginId);
    }

    // ---

    private void initPlugin() {
        try {
            initPluginTopic();
            runPluginMigrations();
            registerPlugin();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.warning("Plugin \"" + pluginName + "\" is not activated");
        }
    }

    private void runPluginMigrations() {
        int dbModelVersion = Integer.parseInt(pluginTopic.getProperty("db_model_version"));
        int requiredDbModelVersion = requiredDBModelVersion();
        int migrationsToRun = requiredDbModelVersion - dbModelVersion;
        logger.info("dbModelVersion=" + dbModelVersion + ", requiredDbModelVersion=" + requiredDbModelVersion +
            " => Running " + migrationsToRun + " plugin migrations");
        for (int i = dbModelVersion + 1; i <= requiredDbModelVersion; i++) {
            dms.runPluginMigration(this, i);
        }
    }
}
