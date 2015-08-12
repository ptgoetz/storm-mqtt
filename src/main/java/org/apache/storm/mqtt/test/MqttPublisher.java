package org.apache.storm.mqtt.test;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Created by tgoetz on 8/4/15.
 */
public class MqttPublisher {

    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient("tcp://localhost:1883", "pahomqttpublish2");
            client.connect();
            MqttMessage message = new MqttMessage();
            message.setPayload("A single message".getBytes());
            client.publish("pahodemo/test", message);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
