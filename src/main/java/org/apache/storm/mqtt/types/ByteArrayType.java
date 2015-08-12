package org.apache.storm.mqtt.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MqttType;
import org.apache.storm.mqtt.TopicMessage;


public class ByteArrayType implements MqttType {
    public Values toValues(TopicMessage message) {
        return new Values(message.getTopic(), message.getMessage());
    }

    public Fields outputFields() {
        return new Fields("topic", "message");
    }
}
