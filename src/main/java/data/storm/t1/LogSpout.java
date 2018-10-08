package data.storm.t1;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class LogSpout implements IRichSpout {
    private SpoutOutputCollector stream;
    private BufferedReader br;
    private FileReader fr;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            fr = new FileReader(new File("/opt/tomorrow/data/trace.log"));
            br = new BufferedReader(fr);
        }catch(Exception e){
            e.printStackTrace();
        }
        stream = spoutOutputCollector;
    }

    @Override
    public void close() {
        try{
            if(br!=null){
                br.close();
            }
            if(fr!=null){
                fr.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        String line;
        try{
            while((line=br.readLine())!=null){
                stream.emit(new Values(line));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
