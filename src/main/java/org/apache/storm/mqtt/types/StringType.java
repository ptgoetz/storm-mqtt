package org.apache.storm.mqtt.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MqttType;
import org.apache.storm.mqtt.TopicMessage;

/**
 * Given a String topic and byte[] message, emits a tuple with fields
 * "topic" and "message", both of which are Strings.
 */
public class StringType implements MqttType {
    public Values toValues(TopicMessage message) {
        return new Values(message.getTopic(), new String(message.getMessage()));
    }

    public Fields outputFields() {
        return new Fields("topic", "message");
    }
}
