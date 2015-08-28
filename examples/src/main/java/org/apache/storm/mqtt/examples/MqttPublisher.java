package org.apache.storm.mqtt.examples;


import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * Created by tgoetz on 8/4/15.
 */
public class MqttPublisher {

    public static void main(String[] args) throws Exception {
            MQTT client = new MQTT();
            client.setHost("tcp://raspberrypi.local:1883");
            client.setClientId("fusesourcepublisher");
            BlockingConnection connection = client.blockingConnection();

            connection.connect();

            connection.publish("testack", "Hello".getBytes(), QoS.AT_LEAST_ONCE, false);

        while(true){
            Thread.sleep(2000);
        }
            //connection.disconnect();

    }
}
