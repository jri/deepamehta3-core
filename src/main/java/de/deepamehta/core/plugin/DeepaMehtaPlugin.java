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

    private ServiceTracker    deepamehtaServiceTracker;
    private DeepaMehtaService deepamehtaService = null;

    private ServiceTracker httpServiceTracker;
    private HttpService httpService = null;

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
        return (Migration) Class.forName(migrationClassName).newInstance();
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
        logger.info("### Starting DeepaMehta plugin bundle \"" + pluginName + "\" ###");
        //
        deepamehtaServiceTracker = createDeepamehtaServiceTracker(context);
        deepamehtaServiceTracker.open();
        //
        httpServiceTracker = createHttpServiceTracker(context);
        httpServiceTracker.open();
    }

    public void stop(BundleContext context) {
        logger.info("### Stopping DeepaMehta plugin bundle \"" + pluginName + "\" ###");
        //
        deepamehtaServiceTracker.close();
        httpServiceTracker.close();
    }



    // *************
    // *** Hooks ***
    // *************



    public int getCodeModelVersion() {
        return 0;
    }

    public String getClientPlugin() {
        return null;
    }

    public void preCreateHook(Topic topic) {
    }

    public void preUpdateHook(Topic topic) {
    }



    // ***********************
    // *** Private Helpers ***
    // ***********************



    private ServiceTracker createDeepamehtaServiceTracker(BundleContext context) {
        return new ServiceTracker(context, DeepaMehtaService.class.getName(), null) {

            @Override
            public Object addingService(ServiceReference serviceRef) {
                logger.info("Adding DeepaMehta Core service");
                deepamehtaService = (DeepaMehtaService) super.addingService(serviceRef);
                initPluginTopic();
                runPluginMigrations();
                registerPlugin();
                return deepamehtaService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == deepamehtaService) {
                    logger.info("Removing DeepaMehta Core service");
                    unregisterPlugin();
                    deepamehtaService = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    private ServiceTracker createHttpServiceTracker(BundleContext context) {
        return new ServiceTracker(context, HttpService.class.getName(), null) {

            @Override
            public Object addingService(ServiceReference serviceRef) {
                logger.info("Adding HTTP service");
                httpService = (HttpService) super.addingService(serviceRef);
                registerResources();
                return httpService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == httpService) {
                    logger.info("Removing HTTP service");
                    unregisterResources();
                    httpService = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    // ---

    private void registerPlugin() {
        logger.info("Registering plugin \"" + pluginId + "\" (" + pluginClass + ")");
        deepamehtaService.registerPlugin(pluginId, this);
    }

    private void unregisterPlugin() {
        logger.info("Unregistering plugin \"" + pluginId + "\" (" + pluginClass + ")");
        deepamehtaService.unregisterPlugin(pluginId);
    }

    // ---

    private void registerResources() {
        logger.info("Registering web resources of plugin \"" + pluginId + "\"");
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
        logger.info("Unregistering web resources of plugin \"" + pluginId + "\"");
        httpService.unregister("/" + pluginId);
    }

    // ---

    private void initPluginTopic() {
        pluginTopic = findPluginTopic();
        if (pluginTopic != null) {
            logger.info("No need to create topic for plugin \"" + pluginId + "\" (already exists)");
        } else {
            logger.info("Creating topic for plugin \"" + pluginId + "\"");
            Map properties = new HashMap();
            properties.put("plugin_id", pluginId);
            properties.put("db_model_version", 0);
            pluginTopic = deepamehtaService.createTopic("Plugin", properties);
        }
    }

    private Topic findPluginTopic() {
        return deepamehtaService.getTopic("plugin_id", pluginId);
    }

    // ---

    private void runPluginMigrations() {
        int db_model_version = (Integer) pluginTopic.getProperty("db_model_version");
        int code_model_version = getCodeModelVersion();
        int migrations_to_run = code_model_version - db_model_version;
        logger.info("db_model_version=" + db_model_version + ", code_model_version=" + code_model_version +
            " => Running " + migrations_to_run + " plugin migrations");
        for (int i = db_model_version + 1; i <= code_model_version; i++) {
            deepamehtaService.runPluginMigration(this, i);
        }
    }
}
