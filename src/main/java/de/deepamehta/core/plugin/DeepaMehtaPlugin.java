package de.deepamehta.core.plugin;

import de.deepamehta.core.service.DeepaMehtaService;

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
import java.util.logging.Logger;



public class DeepaMehtaPlugin implements BundleActivator {

    private ServiceTracker    deepamehtaServiceTracker;
    private DeepaMehtaService deepamehtaService = null;

    private ServiceTracker httpServiceTracker;
    private HttpService httpService = null;

    private Logger logger = Logger.getLogger(getClass().getName());



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    public void start(BundleContext context) {
        logger.info("Starting DeepaMehta Plugin bundle \"" + context.getBundle().getHeaders().get("Bundle-Name") + "\"");
        //
        deepamehtaServiceTracker = createDeepamehtaServiceTracker(context);
        deepamehtaServiceTracker.open();
        //
        httpServiceTracker = createHttpServiceTracker(context);
        httpServiceTracker.open();
    }

    public void stop(BundleContext context) {
        logger.info("Stopping DeepaMehta Plugin bundle \"" + context.getBundle().getHeaders().get("Bundle-Name") + "\"");
        //
        deepamehtaServiceTracker.close();
        httpServiceTracker.close();
    }



    // *************
    // *** Hooks ***
    // *************



    public String getClientPlugin() {
        return null;
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
                registerPlugin(context);
                return deepamehtaService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == deepamehtaService) {
                    logger.info("Removing DeepaMehta Core service");
                    unregisterPlugin(context);
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
                registerResources(context);
                return httpService;
            }

            @Override
            public void removedService(ServiceReference ref, Object service) {
                if (service == httpService) {
                    logger.info("Removing HTTP service");
                    unregisterResources(context);
                    httpService = null;
                }
                super.removedService(ref, service);
            }
        };
    }

    // ---

    private void registerPlugin(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        String pluginClass = (String) context.getBundle().getHeaders().get("Bundle-Activator");
        logger.info("Registering plugin " + pluginId + " (" + pluginClass + ")");
        deepamehtaService.registerPlugin(pluginId, this);
    }

    private void unregisterPlugin(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        String pluginClass = (String) context.getBundle().getHeaders().get("Bundle-Activator");
        logger.info("Unregistering plugin " + pluginId + " (" + pluginClass + ")");
        deepamehtaService.unregisterPlugin(pluginId);
    }

    // ---

    private void registerResources(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        logger.info("Registering web resources of plugin " + pluginId);
        try {
            // Note: to map the bundle root, acording to OSGi API the resource name "/" is to be used.
            // This doesn't work: java.lang.IllegalArgumentException: Malformed resource name [/]
            // Using "" instead works. IMO this is an error in the "Apache Felix Http Jetty" bundle.
            httpService.registerResources("/" + pluginId, "", null);
        } catch (NamespaceException e) {
            throw new RuntimeException(e);
        }
    }

    private void unregisterResources(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        logger.info("Unregistering web resources of plugin " + pluginId);
        httpService.unregister("/" + pluginId);
    }
}
