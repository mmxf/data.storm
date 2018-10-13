package data.storm.t6;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;

import java.util.concurrent.TimeUnit;

public class LocalSubmitter {
    private LocalDRPC drpc;

    private LocalCluster cluster;

    public LocalSubmitter(LocalDRPC drpc,LocalCluster cluster){
        this.drpc = drpc;

        this.cluster = cluster;
    }

    public LocalSubmitter(StormTopology topology,LocalDRPC drpc,LocalCluster cluster){
        this(drpc,cluster);
    }
    public static LocalSubmitter newInstance(){
        return new LocalSubmitter(new LocalDRPC(),new LocalCluster());
    }

    public static Config defaultConfig(){
        return defaultConfig(false);
    }
    public static Config defaultConfig(boolean debug){
        Config config = new Config();
        config.setMaxSpoutPending(20);
        config.setDebug(debug);
        return config;
    }

    public void submit(String name,Config config,StormTopology topology){
        cluster.submitTopology(name,config,topology);
    }

    public void print(int num, int time, TimeUnit unit){
        try {
            for(int i=1;i<=num;i++){
                System.out.println("drpc result:"+drpc.execute("words", "the and apple snow jumped"));
                Thread.sleep(unit.toMillis(time));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
