package data.storm.t3;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Bolt implements IRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String amt = tuple.getStringByField("amt");
            String name = tuple.getStringByField("name");
            collector.ack(tuple);
            collector.emit(tuple,new Values(name,amt));
        } catch (Exception e) {
            collector.fail(tuple);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name1","amt1"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
