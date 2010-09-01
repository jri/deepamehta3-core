package de.deepamehta.core.service;

import de.deepamehta.core.service.Migration;



public abstract class Migration {
    
    protected CoreService dms;

    public void setService(CoreService dms) {
        this.dms = dms;
    }

    public abstract void run();
}
