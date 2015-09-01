package org.apache.storm.mqtt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.Serializable;

/**
 * Represents an object that can be converted to a Storm Tuple from an MQTTMessage,
 * given a MQTT Topic Name and a byte array payload.
 */
public interface MQTTMessageMapper extends Serializable {
    /**
     * Convert a MQTTMessage to a set of Values that can be emitted as a Storm Tuple.
     *
     * @param message An MQTT Message.
     * @return Values representing a Storm Tuple.
     */
    Values toValues(MQTTMessage message);

    /**
     * Returns the list of output fields this Mapper produces.
     *
     * @return the list of output fields this mapper produces.
     */
    Fields outputFields();
}
