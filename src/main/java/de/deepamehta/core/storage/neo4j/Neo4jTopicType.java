package de.deepamehta.core.storage.neo4j;

import de.deepamehta.core.model.DataField;
import de.deepamehta.core.model.TopicType;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



class Neo4jTopicType extends TopicType {

    Node node;

    Neo4jTopicType(Map<String, Object> properties, Node node) {
        super(properties, new ArrayList());
        this.node = node;
    }

    @Override
    public void addDataField(DataField dataField) {
        Node fieldNode = ((Neo4jDataField) dataField).node;
        if (dataFields.size() == 0) {
            node.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE_START);
        } else {
            Neo4jDataField lastField = (Neo4jDataField) dataFields.get(dataFields.size() - 1);
            lastField.node.createRelationshipTo(fieldNode, Neo4jStorage.RelType.SEQUENCE);
        }
        super.addDataField(dataField);
    }
}
