package org.apache.storm.mqtt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttSpout implements IRichSpout, MqttCallback {
    private static final Logger LOG = LoggerFactory.getLogger(MqttSpout.class);
    public static final String MQTT_URL = "org.apache.storm.mqtt.url";
    public static final String MQTT_TOPIC = "org.apache.storm.mqtt.topic";

    private transient MqttClient mqttClient;

    protected transient SpoutOutputCollector collector;
    protected transient TopologyContext context;
    protected transient LinkedBlockingQueue<TopicMessage> incoming;
    protected transient HashMap<Integer, TopicMessage> pending;
    private transient Map conf;
    protected MqttType type;


    public MqttSpout(MqttType type){
        this.type = type;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(this.type.outputFields());
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        this.conf = conf;

        this.incoming = new LinkedBlockingQueue<TopicMessage>();
        this.pending = new HashMap<Integer, TopicMessage>();

        connectMqtt();

    }

    private void connectMqtt(){
        String url = (String)conf.get(MQTT_URL);
        String topic = (String)conf.get(MQTT_TOPIC);
        try {
            this.mqttClient = new MqttClient(url, topic, new MemoryPersistence());
            this.mqttClient.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setWill("/lastWill", "I died.".getBytes(), 0, false);


            this.mqttClient.connect(options);
            this.mqttClient.subscribe((String) conf.get(MQTT_TOPIC));
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.mqttClient.disconnect();
            this.mqttClient.close();
        } catch (MqttException e) {
            e.printStackTrace();
        }

    }

    public void activate() {

    }

    public void deactivate() {

    }

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    public void nextTuple() {
        TopicMessage tm = this.incoming.poll();
        if(tm != null){
            this.collector.emit(this.type.toValues(tm), tm.hashCode());
            this.pending.put(tm.hashCode(), tm);
        }
    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier
     * has been fully processed. Typically, an implementation of this method will take that
     * message off the queue and prevent it from being replayed.
     *
     * @param msgId
     */
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    /**
     * The tuple emitted by this spout with the msgId identifier has failed to be
     * fully processed. Typically, an implementation of this method will put that
     * message back on the queue to be replayed at a later time.
     *
     * @param msgId
     */
    public void fail(Object msgId) {
        try {
            this.incoming.put(this.pending.remove(msgId));
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while re-queueing a value.", e);
        }

    }

    /* ############ MqttCallback implementation ############ */

    /**
     * This method is called when the connection to the server is lost.
     *
     * @param cause the reason behind the loss of connection.
     */
    public void connectionLost(Throwable cause) {
        LOG.warn("MQTT Connection loss.", cause);
        connectMqtt();
    }

    /**
     * This method is called when a message arrives from the server.
     * <p/>
     * <p>
     * This method is invoked synchronously by the MQTT client. An
     * acknowledgment is not sent back to the server until this
     * method returns cleanly.</p>
     * <p>
     * If an implementation of this method throws an <code>Exception</code>, then the
     * client will be shut down.  When the client is next re-connected, any QoS
     * 1 or 2 messages will be redelivered by the server.</p>
     * <p>
     * Any additional messages which arrive while an
     * implementation of this method is running, will build up in memory, and
     * will then back up on the network.</p>
     * <p>
     * If an application needs to persist data, then it
     * should ensure the data is persisted prior to returning from this method, as
     * after returning from this method, the message is considered to have been
     * delivered, and will not be reproducible.</p>
     * <p>
     * It is possible to send a new message within an implementation of this callback
     * (for example, a response to this message), but the implementation must not
     * disconnect the client, as it will be impossible to send an acknowledgment for
     * the message being processed, and a deadlock will occur.</p>
     *
     * @param topic   name of the topic on the message was published to
     * @param message the actual message.
     * @throws Exception if a terminal error has occurred, and the client should be
     *                   shut down.
     */
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        this.incoming.put(new TopicMessage(topic, message.getPayload()));
    }

    /**
     * Called when delivery for a message has been completed, and all
     * acknowledgments have been received. For QoS 0 messages it is
     * called once the message has been handed to the network for
     * delivery. For QoS 1 it is called when PUBACK is received and
     * for QoS 2 when PUBCOMP is received. The token will be the same
     * token as that returned when the message was published.
     *
     * @param token the delivery token associated with the message.
     */
    public void deliveryComplete(IMqttDeliveryToken token) {

    }
}
