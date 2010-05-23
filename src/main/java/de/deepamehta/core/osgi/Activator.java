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
        logger.info("Starting DeepaMehta Core bundle");
        service = new EmbeddedService();
        //
        logger.info("Registering DeepaMehta service");
        context.registerService(DeepaMehtaService.class.getName(), service, null);
    }

    public void stop(BundleContext context) {
        logger.info("Stopping DeepaMehta Core bundle");
        service.shutdown();
    }
}
