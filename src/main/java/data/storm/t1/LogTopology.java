package data.storm.t1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class LogTopology {
    public static void main(String[] args) {
        try{
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout",new LogSpout(),1);
            builder.setBolt("splitBolt",new SplitBolt(),1).shuffleGrouping("spout");
            builder.setBolt("printBolt",new PrintBolt(),1).shuffleGrouping("splitBolt");
            Config conf = new Config();
            conf.setDebug(true);
            if (args != null && args.length > 0) {
                for(String arg:args){
                    System.out.println("LogTopology Arg is "+arg);
                }
                conf.setNumWorkers(3);

                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            }else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("logTopology", conf, builder.createTopology());

                //Thread.sleep(10000);

                //cluster.shutdown();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
