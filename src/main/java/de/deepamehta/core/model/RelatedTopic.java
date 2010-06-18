package de.deepamehta.core.model;



/**
 * A topic data transfer object.
 */
public class RelatedTopic {

    private Topic topic;
    private Relation relation;

    public RelatedTopic() {
    }

    public RelatedTopic(Topic topic, Relation relation) {
        this.topic = topic;
        this.relation = relation;
    }

    // ---

    public Topic getTopic() {
        return topic;
    }

    public Relation getRelation() {
        return relation;
    }

    // ---

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }
}
