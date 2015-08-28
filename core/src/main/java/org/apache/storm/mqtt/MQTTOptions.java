package org.apache.storm.mqtt;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class MQTTOptions implements Serializable {
    private String host = "localhost";
    private int port = 1883;
    private List<String> topics = null;
    private boolean cleanConnection = false;

    private String willTopic;
    private String willPayload;
    private int willQos = 1;
    private boolean willRetain = false;

    private long reconnectDelay = 10;
    private long reconnectDelayMax = 30*1000;
    private double reconnectBackOffMultiplier = 2.0f;
    private long reconnectAttemptsMax = -1;
    private long connectAttemptsMax = -1;

    private String userName = "";
    private String password = "";

    private int qos = 1;

    public MQTTOptions(){}

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public boolean isCleanConnection() {
        return cleanConnection;
    }

    public void setCleanConnection(boolean cleanConnection) {
        this.cleanConnection = cleanConnection;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public String getWillPayload() {
        return willPayload;
    }

    public void setWillPayload(String willPayload) {
        this.willPayload = willPayload;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public long getReconnectDelayMax() {
        return reconnectDelayMax;
    }

    public void setReconnectDelayMax(long reconnectDelayMax) {
        this.reconnectDelayMax = reconnectDelayMax;
    }

    public double getReconnectBackOffMultiplier() {
        return reconnectBackOffMultiplier;
    }

    public void setReconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
        this.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
    }

    public long getReconnectAttemptsMax() {
        return reconnectAttemptsMax;
    }

    public void setReconnectAttemptsMax(long reconnectAttemptsMax) {
        this.reconnectAttemptsMax = reconnectAttemptsMax;
    }

    public long getConnectAttemptsMax() {
        return connectAttemptsMax;
    }

    public void setConnectAttemptsMax(long connectAttemptsMax) {
        this.connectAttemptsMax = connectAttemptsMax;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getQos(){
        return this.qos;
    }

    public void setQos(int qos){
        if(qos < 0 || qos > 2){
            throw new IllegalArgumentException("MQTT QoS must be >= 0 and <= 2");
        }
        this.qos = qos;
    }

    public int getWillQos(){
        return this.willQos;
    }

    public void setWillQos(int qos){
        if(qos < 0 || qos > 2){
            throw new IllegalArgumentException("MQTT Will QoS must be >= 0 and <= 2");
        }
        this.willQos = qos;
    }

    public boolean getWillRetain(){
        return this.willRetain;
    }
    public void setWillRetain(boolean retain){
        this.willRetain = retain;
    }

    public static class Builder {
        private MQTTOptions options = new MQTTOptions();

        public Builder host(String host) {
            this.options.host = host;
            return this;
        }

        public Builder port(int port) {
            this.options.port = port;
            return this;
        }

        public Builder topics(List<String> topics) {
            this.options.topics = topics;
            return this;
        }

        public Builder cleanConnection(boolean cleanConnection) {
            this.options.cleanConnection = cleanConnection;
            return this;
        }

        public Builder willTopic(String willTopic) {
            this.options.willTopic = willTopic;
            return this;
        }

        public Builder willPayload(String willPayload) {
            this.options.willPayload = willPayload;
            return this;
        }

        public Builder willRetain(boolean retain){
            this.options.willRetain = retain;
            return this;
        }

        public Builder willQos(int qos){
            this.options.setWillQos(qos);
            return this;
        }

        public Builder reconnectDelay(long reconnectDelay) {
            this.options.reconnectDelay = reconnectDelay;
            return this;
        }

        public Builder reconnectDelayMax(long reconnectDelayMax) {
            this.options.reconnectDelayMax = reconnectDelayMax;
            return this;
        }

        public Builder reconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
            this.options.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
            return this;
        }

        public Builder reconnectAttemptsMax(long reconnectAttemptsMax) {
            this.options.reconnectAttemptsMax = reconnectAttemptsMax;
            return this;
        }

        public Builder connectAttemptsMax(long connectAttemptsMax) {
            this.options.connectAttemptsMax = connectAttemptsMax;
            return this;
        }

        public Builder userName(String userName) {
            this.options.userName = userName;
            return this;
        }

        public Builder password(String password) {
            this.options.password = password;
            return this;
        }

        public Builder qos(int qos){
            this.options.setQos(qos);
            return this;
        }

        public MQTTOptions build() {
            return this.options;
        }
    }
}
