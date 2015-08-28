package org.apache.storm.mqtt.mappers;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.storm.mqtt.MQTTMessageMapper;
import org.apache.storm.mqtt.MQTTMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class TopicIndexMessageMapper implements MQTTMessageMapper {
    private static final Logger LOG = LoggerFactory.getLogger(TopicIndexMessageMapper.class);
    private int pathIndex = 0;

    public TopicIndexMessageMapper(int index){
        this.pathIndex = index;
    }

    public Values toValues(MQTTMessage message) {
        String topic = message.getTopic();
        String[] pathElements = topic.split("/");
        if(this.pathIndex > pathElements.length){
            LOG.warn("Path is too short. Skipping");
            return new Values(null, topic,  new String(message.getMessage()));

        } else {
            LOG.info("Path: {}", (Object[])pathElements);
            return new Values(pathElements[this.pathIndex], topic,  new String(message.getMessage()));
        }
    }

    public Fields outputFields() {
        return new Fields("id", "topic", "payload");
    }
}
