package de.deepamehta.core.osgi;

import de.deepamehta.core.service.CoreService;
import de.deepamehta.core.impl.EmbeddedService;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

import java.util.logging.Logger;



public class Activator implements BundleActivator {

    private Logger logger = Logger.getLogger(getClass().getName());

    private CoreService service;



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    public void start(BundleContext context) {
        try {
            logger.info("---------- Starting bundle \"DeepaMehta 3 Core\" ----------");
            service = new EmbeddedService();
            //
            logger.info("Registering DeepaMehta core service");
            context.registerService(CoreService.class.getName(), service, null);
        } catch (RuntimeException e) {
            logger.severe("DeepaMehta core service can't be activated. Reason:");
            e.printStackTrace();
            throw e;
        }
    }

    public void stop(BundleContext context) {
        logger.info("---------- Stopping bundle \"DeepaMehta 3 Core\" ----------");
        if (service != null) {
            service.shutdown();
        }
    }
}
