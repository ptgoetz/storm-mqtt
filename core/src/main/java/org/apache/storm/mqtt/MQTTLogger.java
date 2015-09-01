package org.apache.storm.mqtt;

import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around SLF4J logger that allows MQTT messages to be logged.
 */
public class MQTTLogger extends Tracer {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTLogger.class);

    @Override
    public void debug(String message, Object... args) {
        LOG.info(String.format(message, args));
    }

    @Override
    public void onSend(MQTTFrame frame) {
        super.onSend(frame);
    }

    @Override
    public void onReceive(MQTTFrame frame) {
        super.onReceive(frame);
    }
}
