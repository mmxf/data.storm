package data.storm.t2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class Topology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("customSpout",new Spout());
        builder.setBolt("customBolt",new Bolt().withTumblingWindow(BaseWindowedBolt.Duration.seconds(10)),1).shuffleGrouping("customSpout");
        Config conf = new Config();
        conf.setDebug(true);
        try{
            if(args!=null&&args.length>0){
                conf.setNumWorkers(2);
                StormSubmitter.submitTopologyWithProgressBar("customTopology",conf,builder.createTopology());
            }else{
                conf.setMaxTaskParallelism(2);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("customTopology", conf, builder.createTopology());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
