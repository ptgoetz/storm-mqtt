package org.apache.storm.mqtt.mappers;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MQTTMessageMapper;
import org.apache.storm.mqtt.MQTTMessage;

/**
 * Given a String topic and byte[] message, emits a tuple with fields
 * "topic" and "message", both of which are Strings.
 */
public class StringMessageMapper implements MQTTMessageMapper {
    public Values toValues(MQTTMessage message) {
        return new Values(message.getTopic(), new String(message.getMessage()));
    }

    public Fields outputFields() {
        return new Fields("topic", "message");
    }
}
