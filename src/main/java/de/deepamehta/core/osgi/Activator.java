package de.deepamehta.core.osgi;

import de.deepamehta.core.service.DeepaMehtaService;
import de.deepamehta.core.impl.EmbeddedService;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

import java.util.logging.Logger;



public class Activator implements BundleActivator {

    private Logger logger = Logger.getLogger(getClass().getName());

    private DeepaMehtaService service;



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    public void start(BundleContext context) {
        try {
            logger.info("----- Starting DeepaMehta core bundle -----");
            service = new EmbeddedService();
            //
            logger.info("Registering DeepaMehta core service");
            context.registerService(DeepaMehtaService.class.getName(), service, null);
        } catch (Throwable e) {
            e.printStackTrace();
            logger.severe("DeepaMehta core service is not available");
        }
    }

    public void stop(BundleContext context) {
        logger.info("----- Stopping DeepaMehta core bundle -----");
        if (service != null) {
            service.shutdown();
        }
    }
}
