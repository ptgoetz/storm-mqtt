package org.apache.storm.mqtt.test;

import org.fusesource.mqtt.client.*;

/**
 * Created by tgoetz on 8/4/15.
 */
public class MqttSubscriber {


    public static void main(String[] args) throws Exception {

        MQTT client = new MQTT();
        client.setHost("tcp://raspberrypi.local:1883");
        client.setClientId("fusesourcesubscriber");
        client.setCleanSession(false);
        BlockingConnection connection = client.blockingConnection();
        connection.connect();

        Topic[] topics = {new Topic("testack", QoS.AT_LEAST_ONCE)};
        byte[] qoses = connection.subscribe(topics);
        //System.out.println(new String(qoses));

        while(true) {
            Message message = connection.receive();
            System.out.println("Message recieved on topic: " + message.getTopic());
            System.out.println("Payload: " + new String(message.getPayload()));
            message.ack();
        }

        //connection.disconnect();

    }


}
