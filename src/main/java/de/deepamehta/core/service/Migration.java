package de.deepamehta.core.service;

import de.deepamehta.core.service.Migration;



public abstract class Migration {
    
    protected DeepaMehtaService dms;

    public void setDeepaMehtaService(DeepaMehtaService dms) {
        this.dms = dms;
    }

    public abstract void run();
}
