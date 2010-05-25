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



public abstract class DeepaMehtaPlugin implements BundleActivator {

    private ServiceTracker    deepamehtaServiceTracker;
    private DeepaMehtaService deepamehtaService = null;

    private Logger logger = Logger.getLogger(getClass().getName());



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    public void start(BundleContext context) {
        logger.info("Starting DeepaMehta Plugin bundle \"" + context.getBundle().getHeaders().get("Bundle-Name") + "\"");
        //
        deepamehtaServiceTracker = createDeepamehtaServiceTracker(context);
        deepamehtaServiceTracker.open();
    }

    public void stop(BundleContext context) {
        logger.info("Stopping DeepaMehta Plugin bundle \"" + context.getBundle().getHeaders().get("Bundle-Name") + "\"");
        //
        deepamehtaServiceTracker.close();
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

    // ---

    private void registerPlugin(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        String pluginClass = (String) context.getBundle().getHeaders().get("Bundle-Activator");
        logger.info("Registering DeepaMehta Plugin " + pluginClass + " (" + pluginId + ")");
        deepamehtaService.registerPlugin(pluginId, this);
    }

    private void unregisterPlugin(BundleContext context) {
        String pluginId = context.getBundle().getSymbolicName();
        String pluginClass = (String) context.getBundle().getHeaders().get("Bundle-Activator");
        logger.info("Unregistering DeepaMehta Plugin " + pluginClass + " (" + pluginId + ")");
        deepamehtaService.unregisterPlugin(pluginId);
    }
}
