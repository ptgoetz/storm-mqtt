package org.apache.storm.mqtt.test;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Created by tgoetz on 8/4/15.
 */
public class MqttSubscriber {


    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient("tcp://raspberrypi.local:1883", "pahomqttpublish1", new MemoryPersistence());
            MqttCallback callback = new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost.");
                    cause.printStackTrace();
                }

                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("Message arrived on topic: " + topic);
                    System.out.println(new String(message.getPayload()));
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    try {
                        System.out.println("Delivery complete. " + token.getMessage().toString());
                    } catch (Exception e){
                        e.printStackTrace();
                    }

                }
            };
            client.setCallback(callback);

            client.connect();
            client.subscribe("/feeds/photocell");
            try {
                Thread.sleep(120 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

//            client.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


}
