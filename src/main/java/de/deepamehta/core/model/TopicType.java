package de.deepamehta.core.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class TopicType {

    private Map<String, String> properties;
    private List<Map> dataFields;

    public TopicType(Map properties, List dataFields) {
        this.properties = properties;
        this.dataFields = dataFields;
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public Map<String, String> getDataField(int index) {
        return dataFields.get(index);
    }
}
