package org.apache.storm.mqtt;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.mqtt.ssl.KeyStoreLoader;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class MQTTSpout implements IRichSpout, Listener {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTSpout.class);
    public static final String MQTT_URL = "org.apache.storm.mqtt.url";
    public static final String MQTT_TOPIC = "org.apache.storm.mqtt.topic";

    private String topologyName;


    private CallbackConnection connection;

    protected transient SpoutOutputCollector collector;
    protected transient TopologyContext context;
    protected transient LinkedBlockingQueue<MQTTMessage> incoming;
    protected transient HashMap<Long, MQTTMessage> pending;
    private transient Map conf;
    protected MQTTMessageMapper type;
    protected MQTTOptions options;
    protected KeyStoreLoader keyStoreLoader;

    private boolean mqttConnected = false;
    private boolean mqttConnectFailed = false;


    private Long sequence = Long.MIN_VALUE;

    private Long nextId(){
        this.sequence++;
        if(this.sequence == Long.MAX_VALUE){
            this.sequence = Long.MIN_VALUE;
        }
        return this.sequence;
    }


    public MQTTSpout(MQTTMessageMapper type, MQTTOptions options){
        this(type, options, null);
    }

    public MQTTSpout(MQTTMessageMapper type, MQTTOptions options, KeyStoreLoader keyStoreLoader){
        this.type = type;
        this.options = options;
        this.keyStoreLoader = keyStoreLoader;
        URI uri = URI.create(this.options.getUrl());
        if(!uri.getScheme().equalsIgnoreCase("tcp") && this.keyStoreLoader == null){
            throw new IllegalStateException("A TLS/SSL MQTT URL was specified, but no KeyStoreLoader configured. " +
                    "A KeyStoreLoader implementation is required when using TLS/SSL.");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(this.type.outputFields());
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.topologyName = (String)conf.get(Config.TOPOLOGY_NAME);

        this.collector = collector;
        this.context = context;
        this.conf = conf;

        this.incoming = new LinkedBlockingQueue<MQTTMessage>();
        this.pending = new HashMap<Long, MQTTMessage>();

        try {
            connectMqtt();
        } catch (Exception e) {
            this.collector.reportError(e);
            throw new RuntimeException("MQTT Connection failed.", e);
        }

    }

    private void connectMqtt() throws Exception {
        MQTT client = new MQTT();
        URI uri = URI.create(this.options.getUrl());
        if(!uri.getScheme().equalsIgnoreCase("tcp")){
//            throw new IllegalStateException()
        }

        client.setHost(uri);
        client.setClientId(this.topologyName + "-" + this.context.getThisComponentId() + "-" + this.context.getThisTaskId());
        LOG.info("MQTT ClientID: " + client.getClientId().toString());
        client.setCleanSession(this.options.isCleanConnection());

        client.setReconnectDelay(this.options.getReconnectDelay());
        client.setReconnectDelayMax(this.options.getReconnectDelayMax());
        client.setReconnectBackOffMultiplier(this.options.getReconnectBackOffMultiplier());
        client.setConnectAttemptsMax(this.options.getConnectAttemptsMax());
        client.setReconnectAttemptsMax(this.options.getReconnectAttemptsMax());


        client.setUserName(this.options.getUserName());
        client.setPassword(this.options.getPassword());
        client.setTracer(new MQTTLogger());

        if(this.options.getWillTopic() != null && this.options.getWillPayload() != null){
            client.setWillTopic(this.options.getWillTopic());
            client.setWillMessage(this.options.getWillPayload());
            client.setWillRetain(this.options.getWillRetain());
            QoS qos = null;
            switch(this.options.getWillQos()) {
                case 0:
                    qos = QoS.AT_MOST_ONCE;
                    break;
                case 1:
                    qos = QoS.AT_LEAST_ONCE;
                    break;
                case 2:
                    qos = QoS.EXACTLY_ONCE;
                    break;
            }
            client.setWillQos(qos);
        }

        this.connection = client.callbackConnection();
        this.connection.listener(this);
        this.connection.connect(new ConnectCallback());


        while(this.mqttConnected == false && this.mqttConnectFailed == false){
            LOG.info("Waiting for connection...");
            Thread.sleep(500);
        }

        LOG.info("MQTT Connect success --> " + this.mqttConnected);
        LOG.info("MQTT Connect failed --> " + this.mqttConnectFailed);

        if(this.mqttConnected){
            List<String> topicList = this.options.getTopics();
            Topic[] topics = new Topic[topicList.size()];
            QoS qos = null;
            switch(this.options.getQos()) {
                case 0:
                    qos = QoS.AT_MOST_ONCE;
                    break;
                case 1:
                    qos = QoS.AT_LEAST_ONCE;
                    break;
                case 2:
                    qos = QoS.EXACTLY_ONCE;
                    break;
            }
            for(int i = 0;i < topicList.size();i++){
                topics[i] = new Topic(topicList.get(i), qos);
            }
            connection.subscribe(topics, new SubscribeCallback());
        }
    }



    public void close() {
        this.connection.disconnect(new DisconnectCallback());
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
        MQTTMessage tm = this.incoming.poll();
        if(tm != null){
            Long id = nextId();
            this.collector.emit(this.type.toValues(tm), id);
            this.pending.put(id, tm);
        } else {
            Thread.yield();
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
        MQTTMessage msg = this.pending.remove(msgId);
        this.connection.getDispatchQueue().execute(msg.ack());
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
            LOG.warn("Interrupted while re-queueing message.", e);
        }

    }


    // ################# Listener Implementation ######################
    public void onConnected() {
        // this gets called repeatedly for no apparent reason, don't do anything
    }

    public void onDisconnected() {
        // this gets called repeatedly for no apparent reason, don't do anything
    }

    public void onPublish(UTF8Buffer topic, Buffer payload, Runnable ack) {
        LOG.debug("Received message: topic={}, payload={}", topic.toString(), new String(payload.toByteArray()));
        try {
            this.incoming.put(new MQTTMessage(topic.toString(), payload.toByteArray(), ack));
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while queueing an MQTT message.");
        }
    }

    public void onFailure(Throwable throwable) {
        LOG.error("MQTT Connection Failure.", throwable);
        MQTTSpout.this.connection.disconnect(new DisconnectCallback());
        throw new RuntimeException("MQTT Connection failure.", throwable);
    }

    // ################# Connect Callback Implementation ######################
    private class ConnectCallback implements Callback<Void> {
        public void onSuccess(Void v) {
            LOG.info("MQTT Connected. Subscribing to topic...");
            MQTTSpout.this.mqttConnected = true;
        }

        public void onFailure(Throwable throwable) {
            LOG.info("MQTT Connection failed.");
            MQTTSpout.this.mqttConnectFailed = true;
        }
    }

    // ################# Subscribe Callback Implementation ######################
    private class SubscribeCallback implements Callback<byte[]>{
        public void onSuccess(byte[] qos) {
            LOG.info("Subscripton sucessful.");
        }

        public void onFailure(Throwable throwable) {
            LOG.error("MQTT Subscripton failed.", throwable);
            throw new RuntimeException("MQTT Subscribe failed.", throwable);
        }
    }

    // ################# Subscribe Callback Implementation ######################
    private class DisconnectCallback implements Callback<Void>{
        public void onSuccess(Void aVoid) {
            LOG.info("MQTT Disconnect successful.");
        }

        public void onFailure(Throwable throwable) {
            // Disconnects don't fail.
        }
    }

}
