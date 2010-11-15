package de.deepamehta.core.osgi;

import de.deepamehta.core.service.CoreService;
import de.deepamehta.core.impl.EmbeddedService;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

import java.util.logging.Logger;



public class Activator implements BundleActivator, FrameworkListener {

    // ------------------------------------------------------------------------------------------------- Class Variables

    private static CoreService dms;

    // ---------------------------------------------------------------------------------------------- Instance Variables

    private Logger logger = Logger.getLogger(getClass().getName());

    // -------------------------------------------------------------------------------------------------- Public Methods



    // **************************************
    // *** BundleActivator Implementation ***
    // **************************************



    @Override
    public void start(BundleContext context) {
        try {
            logger.info("========== Starting bundle \"DeepaMehta 3 Core\" ==========");
            dms = new EmbeddedService();
            //
            logger.info("Registering DeepaMehta core service");
            context.registerService(CoreService.class.getName(), dms, null);
            //
            context.addFrameworkListener(this);
        } catch (RuntimeException e) {
            logger.severe("DeepaMehta core service can't be activated. Reason:");
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void stop(BundleContext context) {
        logger.info("========== Stopping bundle \"DeepaMehta 3 Core\" ==========");
        if (dms != null) {
            dms.shutdown();
        }
    }



    // ****************************************
    // *** FrameworkListener Implementation ***
    // ****************************************



    @Override
    public void frameworkEvent(FrameworkEvent event) {
        switch (event.getType()) {
        case FrameworkEvent.STARTED:
            logger.info("########## OSGi framework STARTED ##########");
            dms.startup();
            break;
        case FrameworkEvent.STOPPED:
            logger.info("########## OSGi framework STOPPED ##########");
            break;
        }
    }



    // **************
    // *** Helper ***
    // **************



    public static CoreService getService() {
        // CoreService dms = (CoreService) deepamehtaServiceTracker.getService();
        if (dms == null) {
            throw new RuntimeException("DeepaMehta core service is currently not available");
        }
        return dms;
    }
}
