package org.apache.storm.mqtt;

import org.apache.storm.commons.lang.builder.EqualsBuilder;
import org.apache.storm.commons.lang.builder.HashCodeBuilder;

/**
 * Represents an MQTT Message consisting of a topic string (e.g. "/users/ptgoetz/office/thermostat")
 * and a byte array message/payload.
 *
 */
public class MQTTMessage {
    private String topic;
    private byte[] message;
    private Runnable ack;

    MQTTMessage(String topic, byte[] message, Runnable ack){
        this.topic = topic;
        this.message = message;
        this.ack = ack;
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
        MQTTMessage tm = (MQTTMessage)obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(this.topic, tm.topic)
                .append(this.message, tm.message)
                .isEquals();
    }

    Runnable ack(){
        return this.ack;
    }
}