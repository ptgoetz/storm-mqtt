package org.apache.storm.mqtt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.Serializable;

/**
 * Represents an object that can be converted to a Storm Tuple from an MqttMessage,
 * given a MQTT Topic Name and a byte array payload.
 */
public interface MqttType extends Serializable {
    Values toValues(TopicMessage message);
    Fields outputFields();
}
