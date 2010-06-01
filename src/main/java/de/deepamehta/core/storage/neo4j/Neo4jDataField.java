package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;

import org.neo4j.graphdb.Node;

import java.util.Map;



class Neo4jDataField extends DataField {

    Node node;

    Neo4jDataField(Map<String, String> properties, Node node) {
        super(properties);
        this.node = node;
    }
}
