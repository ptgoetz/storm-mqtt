package org.apache.storm.mqtt;

import org.apache.storm.commons.lang.builder.EqualsBuilder;
import org.apache.storm.commons.lang.builder.HashCodeBuilder;

public class TopicMessage {
    private String topic;
    private byte[] message;

    TopicMessage(String topic, byte[] message){
        this.topic = topic;
        this.message = message;
    }

    public byte[] getMessage(){
        return this.message;
    }

    public String getTopic(){
        return this.topic;
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 123)
                .append(this.topic)
                .append(this.message)
                .toHashCode();
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        TopicMessage tm = (TopicMessage)obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(this.topic, tm.topic)
                .append(this.message, tm.message)
                .isEquals();
    }
}