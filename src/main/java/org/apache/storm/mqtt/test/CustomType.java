package org.apache.storm.mqtt.test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MqttType;
import org.apache.storm.mqtt.TopicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a topic name: "/users/{user}/{location}/{deviceId}"
 * and a payload of "{temperature}/{humidity}"
 * emits a tuple containing user(String), deviceId(String), location(String), temperature(float), humidity(float)
 *
 */
public class CustomType implements MqttType {
    private static final Logger LOG = LoggerFactory.getLogger(CustomType.class);


    public Values toValues(TopicMessage message) {
        String topic = message.getTopic();
        String[] topicElements = topic.split("/");
        String[] payloadElements = new String(message.getMessage()).split("/");

        return new Values(topicElements[2], topicElements[4], topicElements[3], Float.parseFloat(payloadElements[0]), Float.parseFloat(payloadElements[1]));
    }

    public Fields outputFields() {
        return new Fields("user", "deviceId", "location", "temperature", "humidity");
    }
}
