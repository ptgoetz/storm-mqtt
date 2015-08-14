package org.apache.storm.mqtt.mappers;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MQTTMessageMapper;
import org.apache.storm.mqtt.MQTTMessage;


public class ByteArrayMessageMapper implements MQTTMessageMapper {
    public Values toValues(MQTTMessage message) {
        return new Values(message.getTopic(), message.getMessage());
    }

    public Fields outputFields() {
        return new Fields("topic", "message");
    }
}
