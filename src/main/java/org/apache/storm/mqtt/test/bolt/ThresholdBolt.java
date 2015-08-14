package org.apache.storm.mqtt.test.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThresholdBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ThresholdBolt.class);
    private static enum State {
        BELOW, ABOVE;
    }

    private State last = State.BELOW;
    private int threshold;

    public ThresholdBolt(int threshold){
        this.threshold = threshold;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        int val = tuple.getIntegerByField("radiation");
        String zone = tuple.getStringByField("zone");
        State newState = val < this.threshold ? State.BELOW : State.ABOVE;
        boolean stateChange = this.last != newState;
        if(stateChange) {
            LOG.info("### State Change ###");
            collector.emit(new Values(zone, val, stateChange, threshold));
        }
        this.last = newState;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zone", "radiation", "stateChange", "threshold"));
    }
}
