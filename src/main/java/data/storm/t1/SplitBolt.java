package data.storm.t1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt implements IRichBolt {
    OutputCollector stream;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stream = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("Enter SplitBolt Class in execute method");
        String line = tuple.getStringByField("line");
        String[] cells = line.split("\t");
        stream.emit(new Values(cells[0],cells[1],cells[2]));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("createTime","productName","price"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
