package org.apache.storm.mqtt.test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MqttType;
import org.apache.storm.mqtt.TopicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class TopicIndexType implements MqttType {
    private static final Logger LOG = LoggerFactory.getLogger(TopicIndexType.class);
    private int pathIndex = 0;

    public TopicIndexType(int index){
        this.pathIndex = index;
    }

    public Values toValues(TopicMessage message) {
        String topic = message.getTopic();
        String[] pathElements = topic.split("/");
        if(this.pathIndex > pathElements.length){
            LOG.warn("Path is too short. Skipping");
            return new Values(null, topic,  new String(message.getMessage()));

        } else {
            LOG.info("Path: {}", pathElements);
            return new Values(pathElements[this.pathIndex], topic,  new String(message.getMessage()));
        }
    }

    public Fields outputFields() {
        return new Fields("id", "topic", "payload");
    }
}
