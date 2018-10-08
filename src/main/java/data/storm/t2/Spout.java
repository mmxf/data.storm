package data.storm.t2;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class Spout implements IRichSpout {
    private SpoutOutputCollector stream;

    private Random random = new Random();

    private String[] amtList = {"10","15","20","25","30","35","40"};

    private String[] dateList = {"20181001","20181002","20181003","20181004","20181005","20181006","20181007","20181008"};

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.stream = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        String amt = amtList[random.nextInt(amtList.length)];

        String date = dateList[random.nextInt(dateList.length)];

        stream.emit(new Values(amt,date));


    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("amt","date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
