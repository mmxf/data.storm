package data.storm.t3;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new Spout());
        builder.setBolt("bolt",new Bolt(),1).shuffleGrouping("spout");
        builder.setBolt("printBolt",new PrintBolt(),1).shuffleGrouping("bolt");
        Config conf = new Config();
        conf.setDebug(true);
        try{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("topology", conf, builder.createTopology());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
