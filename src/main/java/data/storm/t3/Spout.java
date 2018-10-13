package data.storm.t3;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Spout implements IRichSpout {
    private SpoutOutputCollector collector;

    private ConcurrentHashMap<Object,Values> pending;

    private ConcurrentHashMap<Object,Integer> failPending;

    private Random random = new Random();

    private String[] amtList = {"10","15","20","25","30","35","40","45","50","55","60","65","70","75"};

    private String[] nameList = {"手表","鼠标","键盘","U盘","机箱","硬盘","显卡","主板","CPU","内存条"};
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.pending = new ConcurrentHashMap<>();
        this.failPending = new ConcurrentHashMap<>();
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
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        UUID msgId = UUID.randomUUID();
        String amt = amtList[random.nextInt(amtList.length)];

        String name = nameList[random.nextInt(nameList.length)];

        Values values = new Values(name,amt);

        pending.put(msgId,values);

        collector.emit(values,msgId);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("pending size is "+pending.size());
        System.out.println("Spout ack:"+msgId.toString()+"---"+msgId.getClass());
        this.pending.remove(msgId);
        System.out.println("pending delete after size is "+pending.size());
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Spout fail:"+msgId.toString());
        Integer failCount = failPending.get(msgId);
        if(failCount==null){
            failCount = 0;
        }
        failCount ++;
        if(failCount>3){
            failPending.remove(msgId);
        }else{
            failPending.put(msgId,failCount);
            this.collector.emit(this.pending.get(msgId),msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name","amt"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
